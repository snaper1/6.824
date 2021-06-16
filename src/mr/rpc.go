/**********************************************
*	author: Snaper
*	decs: 	MapReduce Rpc Defination
**********************************************/
package mr

import "os"
import "strconv"

//
// 
// request and reply for an RPC.
//


type MrRpcArgs struct {
	

}

type MrRpcReply struct {
	//TaskTpe:
	//1. MapTask
	//2. ReduceTask
	TaskType int	
	FilePath string //input file path
	TaskSeqNum int  //Task number for output
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
