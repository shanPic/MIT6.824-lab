package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskTypeEnum int8
const (
	Task_Type_Map    TaskTypeEnum = 0
	Task_Type_Reduce TaskTypeEnum = 1
	Task_Type_Wait   TaskTypeEnum = 2
	Task_Type_Finished	 TaskTypeEnum = 3
)

// for Master.GetWorkerID()
type GetIDArgs struct {
}

type GetIDReply struct {
	WorkerID int64 // worker's id
}

// for Master.RequestTask()
type ReqArgs struct {
	WorkID int64
}

type ReqReply struct {
	TaskType  TaskTypeEnum // 分配的任务类型（Map/Reduce）
	TaskID    int64        // 分配的任务的ID
	ReduceID  int
	FilesName []string     // 分配的任务对应的文件名
}

// for Master.CompleteTask()
type CompleteArgs struct {
	WorkerID int64 // Worker ID
	TaskID   int64 // 任务ID
	//todo 任务完成的状态

	/* 任务完成后输出的结果
	// 1. Map任务 [key]:ReduceID [value]:输出的文件名
	// 2. Reduce任务 [key]:文件序号（无特殊含义） [value]:输出的文件名
	*/
	FilesName map[int]string
}

type CompleteReply struct {
	HasNextTask bool // 是否仍然继续等待下个任务 [true]:继续等待下个任务 [false]:再无下个任务
}

// for Master.PongWorker()
type PingArgs struct {
	WorkerID int64
}

type PingReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
