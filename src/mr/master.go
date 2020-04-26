package mr

import (
    "errors"
    "fmt"
    "log"
)
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
    Worker_State_Wait	 WorkerStateEnum = 3
)

type MasterStateEnum int8
const (
	Master_State_Wait	MasterStateEnum = 0
    Master_State_Map	MasterStateEnum = 1
    Master_State_Reduce MasterStateEnum = 2
    Master_State_Done	MasterStateEnum = 3
)

type FileStateEnum int8
const (
	File_State_Wait		FileStateEnum = 0
	File_State_Doing	FileStateEnum = 1
	File_State_Done		FileStateEnum = 2
)

type WorkerState struct {
    state    WorkerStateEnum // state
    map_file string          // map tasks分配的file name
    task_ID  int64
    task_bgein_time int64 // todo
}

type InterFilesDescriptor struct {
    files_name 	[]string
    state		FileStateEnum
}

type Master struct {
    n_reduce	int  // 执行reduce节点的个数

    cur_state_mutex sync.Mutex
    cur_state	MasterStateEnum // Master的当前状态机

    input_files_mutex sync.Mutex
    input_files       map[string]FileStateEnum // 输入的文件list //todo 使用两个list的优化选择的速度

    intermediate_files_mutex sync.Mutex
    intermediate_files map[int]*InterFilesDescriptor // Map过程产生的中间文件列表, key为Reduce序号，value为文件名列表

    worker_status_mutex sync.Mutex
    worker_status map[int64]*WorkerState // 维护Worker的当前状态

    worker_ID_mutex sync.Mutex
    cur_worker_ID   int64 // 当前递增到的worker id
}

func (m *Master) isMapFinished() bool {
	ret := true
	m.input_files_mutex.Lock()
	defer m.input_files_mutex.Unlock()
	for _, v := range m.input_files {
		if v != File_State_Done {
			ret = false
			break
		}
	}
	return ret
}

func (m *Master) isReduceFinished() bool {
	ret := true
	m.intermediate_files_mutex.Lock()
	defer m.intermediate_files_mutex.Unlock()
	for _, v := range m.intermediate_files {
		if v.state != File_State_Done {
			ret = false
			break
		}
	}
	return ret
}

func (m *Master) getNewMapTask() (string, error) {
    m.input_files_mutex.Lock()
    defer m.input_files_mutex.Unlock()
    for k, v := range m.input_files {
        if v == File_State_Wait {
            return k, nil
        }
    }
    return "", errors.New("not have waitting task input file!")
}

func (m *Master) getNewReduceTask() (*InterFilesDescriptor, error) {

    return nil, errors.New("not have a new reducetask!")
}

func (m *Master) GetWorkerID(args *GetIDArgs, reply *GetIDReply) error {
    m.worker_ID_mutex.Lock()
    reply.WorkerID = m.cur_worker_ID
    m.cur_worker_ID++
    m.worker_ID_mutex.Unlock()

    m.worker_status_mutex.Lock()
    m.worker_status[reply.WorkerID] = &WorkerState{
    	state: Worker_State_Wait,
	}
    m.worker_status_mutex.Unlock()
    return nil
}

func (m *Master) RequestTask(args *ReqArgs, reply *ReqReply) error {
	// 1. 判断Master状态机状态
		// 1.1 Map阶段
			// 1.1.1 从输入文件中选择一个还未处理的文件，赋值给reply
			// 1.1.2 维护Worker当前状态（Work_status）
		// 1.2 Reduce
			// 1.2.1 从中间文件列表中选择一组还未处理的文件，赋值给reply
			// 1.2.1 维护Worker当前状态
		// 1.3 Done
			// 返回结束任务的消息

    // 判断此worker的上个任务是否完成
    {
        m.worker_status_mutex.Lock()
        if m.worker_status[args.WorkID].state != Worker_State_Wait {
            fmt.Println("send wait task")
            reply.TaskType = Task_Type_Wait
            m.worker_status_mutex.Unlock()
            return nil
        }
        m.worker_status_mutex.Unlock()
    }



    m.cur_state_mutex.Lock()
    cp_cur_state := m.cur_state
    fmt.Printf("cur_state:%v\n", m.cur_state)
    m.cur_state_mutex.Unlock()
    switch  cp_cur_state {
    case Master_State_Wait: {
        m.cur_state_mutex.Lock()
        m.cur_state = Master_State_Map
        m.cur_state_mutex.Unlock()
    }
    fallthrough
    case Master_State_Map: {
        map_file, err := m.getNewMapTask()
        if err != nil {
            reply.TaskType = Task_Type_Wait
        } else {
            reply.TaskType = Task_Type_Map

            m.input_files_mutex.Lock()
            m.input_files[map_file] = File_State_Doing
            m.input_files_mutex.Unlock()

            fmt.Println(map_file) // for test

            reply.FilesName = make([]string, 0)
            reply.FilesName = append(reply.FilesName, map_file)

            if !m.isMapFinished() {
                m.worker_status_mutex.Lock()
                m.worker_status[args.WorkID].state = Worker_State_Map
                m.worker_status[args.WorkID].map_file = map_file
                // todo m.worker_status[args.WorkID].task_ID
                //todo m.worker_status[args.WorkID].task_bgein_time
                m.worker_status_mutex.Unlock()
            }
        }
    }
    case Master_State_Reduce: {
         reduce_files, err := m.getNewReduceTask()
         if err != nil {
             reply.TaskType = Task_Type_Wait
         } else {
             reply.TaskType = Task_Type_Reduce
             reply.FilesName = reduce_files.files_name
             reduce_files.state = File_State_Doing

             if !m.isReduceFinished() {
                 m.worker_status_mutex.Lock()
                 m.worker_status[args.WorkID].state = Worker_State_Reduce
                 // todo m.worker_status[args.WorkID].task_ID
                 // todo m.worker_status[args.WorkID].task_bgein_time
                 m.worker_status_mutex.Unlock()
             }
         }

    }
    case Master_State_Done: {
        reply.TaskType = Task_Type_Finished

        m.worker_status_mutex.Lock()
        m.worker_status[args.WorkID].state = Worker_State_Wait
        m.worker_status_mutex.Unlock()
    }
    }
    return nil
}

func (m *Master) CompleteTask(args *CompleteArgs, reply *CompleteReply) error {
	// 1. 判断Master状态机状态
		// 1.1 Map阶段
			// 1.1.1 判断输入是否为Map结果
			// 1.1.2 修改输入文件列表中的相应状态为done。如果发现文件状态为wait，则表明此文件的上个任务已经超时。
			// 1.1.3 维护Worker状态
		// 1.2 Reduce阶段
			// 1.2.1 判断输入是否为Reduce结果
			// 1.2.2 修改中间文件列表中的相应状态为done。
			// 1.2.3 维护Worker状态
		// 1.3 Done阶段
			// 返回结束任务的消息 或 不做处理
	// 2. 维护Master状态机状态
		// 使用isMapFinished()或isReduceFinished()维护状态
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

    m.cur_state_mutex.Lock()
    defer m.cur_state_mutex.Unlock()
    if m.cur_state == Master_State_Done {
		ret = true
	}

    return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
    m := Master{
        n_reduce:           1,
        cur_worker_ID:      0,
        input_files:        make(map[string]FileStateEnum),
		intermediate_files: make(map[int]*InterFilesDescriptor),
        worker_status:      make(map[int64]*WorkerState),
    }

    m.input_files_mutex.Lock()
    for _, file := range files {
        m.input_files[file] = File_State_Wait
    }
    //fmt.Println(m.input_files)
    m.input_files_mutex.Unlock()

    // Your code here.

    m.server()
    return &m
}
