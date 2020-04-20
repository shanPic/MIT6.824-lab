package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type WorkerStateEnum int8

const (
	Worker_State_Map     WorkerStateEnum = 0
	Worker_State_Reduce  WorkerStateEnum = 1
	Worker_State_Timeout WorkerStateEnum = 2
)

type WorkerState struct {
	state    WorkerStateEnum // state
	map_file string          // map tasks分配的file name
	task_ID  int64
}

type Master struct {
	n_reduce     int      // 执行reduce节点的个数
	intput_files []string // 输入的文件list

	worker_status map[int64]WorkerState // 维护Worker的当前状态

	worker_ID_mutex sync.Mutex
	cur_worker_ID   int64 // 当前递增到的worker id
}

func (m *Master) GetWorkerID(args *GetIDArgs, reply *GetIDReply) error {
	m.worker_ID_mutex.Lock()
	reply.WorkerID = m.cur_worker_ID
	m.cur_worker_ID++
	m.worker_ID_mutex.Unlock()
	return nil
}

func (m *Master) RequestTask(args *ReqArgs, reply *ReqReply) error {

	return nil
}

func (m *Master) CompleteTask(args *CompleteArgs, reply *CompleteReply) error {

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
