/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:18
 * @LastEditTime: 2021-06-19 17:39:12
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
				call("Coordinate.CompleteTask", &MrRpcArgs{reply.TaskType, outPutFiles, reply.MTask.TaskSeqNum}, &MrRpcReply{})

			} else {
				log.Printf("[ERROR] MapTask no.%d failed, redo work", reply.MTask.TaskSeqNum)
			}

		case REDUCE_TASK:
			reduceProcess()
		default:
			return
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
		log.Fatalf("cannot open %v", reply.MTask.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.MTask.Filename)
	}
	file.Close()
	kva := mapf(reply.MTask.Filename, string(content))
	return writeIntoFile(kva, reply.MTask.TaskSeqNum)

}

/**
 * @name: reduceProcess
 * @desc:
 * @param {*} string
 * @param {*} string
 * @return {*}
 */
func reduceProcess(reducef func(string, []string) string, reply MrRpcReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.RTask.MTaskNum; i++ {
		filename := fmt.Sprintf("map-out-partition-%v-%v", i, reply.RTask.TaskSeqNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		intermediate = append(intermediate, content...)
	}

}

/**
 * @name:  writeIntoFile
 * @desc:  把map结果输出,输出文件为map-out-partition-mapSeq-reduceSeq
 * @param {[]KeyValue} kvs 输出的键值对
 * @param {int} fileSeqNum 文件的序号
 * @return {*}执行是否成功
 */
func writeIntoFile(kvs []KeyValue, fileSeqNum int) ([]string, bool) {
	var outputFileNames []string
	for _, kv := range kvs {
		reduceSeqNum := ihash(kv.Key) % N_REDUCE
		ofile := fmt.Sprintf("map-out-partition-%d-%d", fileSeqNum, reduceSeqNum)
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
	if ok == false {
		log.Print("[FATAL] call SendTask failed, work done")
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
