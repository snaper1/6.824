/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:18
 * @LastEditTime: 2021-06-17 01:02:22
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
	args := MrRpcArgs{}
	reply := MrRpcReply{}
	call("Coordinator.SendTask", &args, &reply)

	if reply.TaskType == MapTask {
		mapProcess(mapf, &reply)

	} else {
		reduceProcess()
	}

}

/**
 * @name: mapProcess
 * @desc: 对文件进行读入，并执行map方法
 * @param mapf, MrRpcReply
 * @return 输出的文件集合和是否正常运行
 */

func mapProcess(mapf func(string, string) []KeyValue, reply *MrRpcReply) ([]string, bool) {

	file, err := os.Open(reply.FilePath)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FilePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FilePath)
	}
	file.Close()
	kva := mapf(reply.FilePath, string(content))
	return writeIntoFile(kva, reply.TaskSeqNum)

}

/**
 * @name:
 * @desc:
 * @param {*}
 * @return {*}
 */
func reduceProcess() {

}

/**
 * @name:  writeIntoFile
 * @desc:	把map结果输出
 * @param 输出的键值对  文件的序号
 * @return 文件名   执行是否成功
 */

func writeIntoFile(kvs []KeyValue, fileSeqNum int) ([]string, bool) {
	var outputFileNames []string
	for _, kv := range kvs {
		reduceSeqNum := ihash(kv.Key) % NReduce
		ofile := fmt.Sprintf("map-out-partition-%d-%d", fileSeqNum, reduceSeqNum)
		f, _ := os.OpenFile(ofile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
		enc := json.NewEncoder(f)
		err := enc.Encode(kv)
		f.Close()
		if err != nil {
			return outputFileNames, false
		}
		outputFileNames = append(outputFileNames, ofile)
	}

	return outputFileNames, true
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
