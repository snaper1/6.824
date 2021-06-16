/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:18
 * @LastEditTime: 2021-06-16 14:49:25
 */

package mr

import (
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func mapProcess(mapf func(string, string) []KeyValue, reply *MrRpcReply) (string, error) {

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
	return writeIntoFile(kva)

}

func reduceProcess() {

}

/**
 * @name:  writeIntoFile
 * @desc:  
 * @param {type}
 * @return {type}
 */

func writeIntoFile([]KeyValue) (string, error) {

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
