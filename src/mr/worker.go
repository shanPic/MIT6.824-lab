package mr

import (
	"errors"
	"fmt"
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

		if !call("Master.RequestTask", &req_args, &req_reply) {
			time.Sleep(1 * time.Second)
			continue
		}

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
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
