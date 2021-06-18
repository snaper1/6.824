/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:17
 * @LastEditTime: 2021-06-19 00:27:37
 */

package mr

import (
	"os"
	"strconv"
)

//
//
// request and reply for an RPC.
//

type MrRpcArgs struct {
	TaskType   int
	FilePaths  []string
	TaskSeqNum int
}

type MrRpcReply struct {
	//TaskTpe:
	//1. MapTask
	//2. ReduceTask
	TaskType int
	MTask    MapTask
	RTask    ReduceTask
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
