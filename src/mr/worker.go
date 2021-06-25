/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:18
 * @LastEditTime: 2021-06-26 01:20:35
 */

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := getTask()
		if reply == (MrRpcReply{}) {
			return
		}
		switch reply.TaskType {

		case MAP_TASK:
			outPutFiles, ok := mapProcess(mapf, reply)
			if ok == true {
				log.Printf("MapTask no.%d success!", reply.MTask.TaskSeqNum)

				call("Coordinator.CompleteTask", &MrRpcArgs{reply.TaskType, outPutFiles, reply.MTask.TaskSeqNum}, &MrRpcReply{})

			} else {
				log.Printf("[ERROR] MapTask no.%d failed, redo work", reply.MTask.TaskSeqNum)
				call("Coordinator.RedoWork", &MrRpcArgs{reply.TaskType, []string{reply.MTask.Filename}, reply.MTask.TaskSeqNum}, &MrRpcReply{})
			}

		case REDUCE_TASK:
			outPutFile, ok := reduceProcess(reducef, reply)
			if ok == true {
				log.Printf("ReduceTask no.%d success!", reply.RTask.TaskSeqNum)
				call("Coordinator.CompleteTask", &MrRpcArgs{reply.TaskType, []string{outPutFile}, reply.RTask.TaskSeqNum}, &MrRpcReply{})

			} else {
				log.Printf("[ERROR] ReduceTask no.%d failed, redo work", reply.RTask.TaskSeqNum)
				call("Coordinator.RedoWork", &MrRpcArgs{reply.TaskType, []string{}, reply.RTask.TaskSeqNum}, &MrRpcArgs{})

			}
		case DONE:
			return

		default:
			time.Sleep(time.Millisecond)
		}

	}

}

/**
 * @name: mapProcess
 * @desc: 对文件进行读入，并执行map方法
 * @param {*} string mapf
 * @param {*} string MrRpcReply
 * @return {*} 输出的文件集合和是否正常运行
 */
func mapProcess(mapf func(string, string) []KeyValue, reply MrRpcReply) ([]string, bool) {

	file, err := os.Open(reply.MTask.Filename)
	if err != nil {
		//log.Printf("cannot open %s", reply.MTask.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %s", reply.MTask.Filename)
	}
	file.Close()
	kva := mapf(reply.MTask.Filename, string(content))
	return writeIntoFile(kva, reply.MTask.TaskSeqNum)

}

/**
 * @name:
 * @desc:
 * @param {*} string
 * @param {*} string
 * @return {*}
 */
func reduceProcess(reducef func(string, []string) string, reply MrRpcReply) (string, bool) {
	oname := fmt.Sprintf("mr-out-%v", reply.RTask.TaskSeqNum)
	ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	intermediate := []KeyValue{}
	for i := 0; i < reply.RTask.MTaskNum; i++ {
		//read file

		filename := fmt.Sprintf("mr-%v-%v", i, reply.RTask.TaskSeqNum)
		file, err := os.OpenFile(filename, os.O_RDONLY, 0777)
		if err != nil {
			//log.Printf("[warning] cannot open %s", filename)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		os.Remove(filename)

	}
	if len(intermediate) == 0 {
		return oname, true
	}
	sort.Sort(ByKey(intermediate))
	//reduce
	var values []string
	values = append(values, intermediate[0].Value)
	for j := 1; j < len(intermediate); j++ {
		if intermediate[j].Key == intermediate[j-1].Key {
			values = append(values, intermediate[j].Value)
		} else {
			output := reducef(intermediate[j-1].Key, values)
			fmt.Fprintf(ofile, "%v %v\n", intermediate[j-1].Key, output)
			values = values[0:0] //清空切片
			values = append(values, intermediate[j].Value)
		}
	}
	if len(values) != 0 {
		length := len(intermediate)
		output := reducef(intermediate[length-1].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[length-1].Key, output)

	}
	return oname, true

}

/**
 * @name:  writeIntoFile
 * @desc:  把map结果输出,输出文件为mr-mapSeq-reduceSeq
 * @param {[]KeyValue} kvs 输出的键值对
 * @param {int} fileSeqNum 文件的序号
 * @return {*}执行是否成功
 */
func writeIntoFile(kvs []KeyValue, fileSeqNum int) ([]string, bool) {
	var outputFileNames []string
	for _, kv := range kvs {
		reduceSeqNum := ihash(kv.Key) % N_REDUCE
		ofile := fmt.Sprintf("mr-%d-%d", fileSeqNum, reduceSeqNum)
		f, _ := os.OpenFile(ofile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
		enc := json.NewEncoder(f)
		err := enc.Encode(kv)

		f.Close()
		if err != nil {
			log.Printf("[ERROR] MapWorker no.%d write error , %s", fileSeqNum, err)
			for _, outputFileName := range outputFileNames {
				os.Remove(outputFileName)
			}
			return nil, false
		}
		outputFileNames = append(outputFileNames, ofile)
	}

	return outputFileNames, true
}

/**
 * @name: getTask
 * @desc: 获取任务
 * @param {*}
 * @return {*}
 */
func getTask() MrRpcReply {
	args := MrRpcArgs{}
	reply := MrRpcReply{}
	ok := call("Coordinator.SendTask", &args, &reply)
	if !ok {
		return MrRpcReply{}
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return false
	}
	return true
}
