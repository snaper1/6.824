/**********************************************
*	author: Snaper
*	decs: 	MapReduce Rpc Server
**********************************************/
package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
)


const(
	MapTask = 1
	ReduceTask = 2 

)
	

type Coordinator struct {
	// Your definitions here.

}



// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) SendTask(args *MrRpcArgs, reply *MrRpcReply) error {

	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}



//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	

	c.server()
	return &c
}
