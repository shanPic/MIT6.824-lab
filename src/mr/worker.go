package mr

import (
	"errors"
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	workerID, err := CallGetWorkerID()

	if err != nil {
		log.Fatal("get worker ID failed!")
		return
	}

	for {
		req_args := ReqArgs{workerID}
		req_reply := ReqReply{}

		var complete_args CompleteArgs
		var complete_reply CompleteReply

		complete_args.WorkerID = workerID

		out_files := make(map[int]string, 1)
		if  call("Master.RequestTask", &req_args, &req_reply) {
			complete_args.TaskID = req_reply.TaskID
			if req_reply.TaskType == Task_Type_Map {
				fmt.Printf("task type is map, task file name is %v\n", req_reply.FilesName)
				// todo do map
			}
			if req_reply.TaskType == Task_Type_Reduce {
				fmt.Printf("task type is map, task file name is %v\n", req_reply.FilesName)

				// todo do reduce
				out_file_name := "mr-out-" + string(req_reply.ReduceID)
				os.Create(out_file_name)

				out_files[0] = out_file_name
			}
			if req_reply.TaskType == Task_Type_Wait {
				fmt.Printf("task type is wait\n")
				// todo do wait
			}
		} else {
			fmt.Printf("request task failed!\n")
		}

		complete_args.FilesName = out_files
		if call("Master.CompleteTask", &complete_args, &complete_reply) {
			if !complete_reply.HasNextTask {
				fmt.Println("not have task, break")
				break
			}
		} else {
			break
		}

		time.Sleep(1 * time.Second)

		// todo
	}

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

// Call GetWorkID test
func CallGetWorkerID() (int64, error) {
	args := GetIDArgs{}
	reply := GetIDReply{}

	if call("Master.GetWorkerID", &args, &reply) {
		fmt.Printf("reply GetworkID:%v\n", reply.WorkerID)
		return reply.WorkerID, nil
	}

	return 0, errors.New("rpc call failed!")
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		fmt.Println(err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	fmt.Println("request failed!")
	return false
}
