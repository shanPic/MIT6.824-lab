package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

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
				input_file, err := os.Open(req_reply.FilesName[0])
				defer input_file.Close()
				if err != nil {
					log.Fatalf("open file %v failed", req_reply.FilesName[0])
				}

				content, err := ioutil.ReadAll(input_file)

				map_result := mapf(req_reply.FilesName[0], string(content))
				map_out_files := mapResultWriter(map_result, req_reply.TaskID)
				out_files = *map_out_files
			}
			if req_reply.TaskType == Task_Type_Reduce {
				fmt.Printf("task type is reduce, task file name is %v\n", req_reply.FilesName)

				reduce_data := parseMapResult(req_reply.FilesName)

				total_reduce_result := make([]string, 0)
				for k, v := range reduce_data {
					cur_k_reduce_result := reducef(k, v)
					format_reduce_result := fmt.Sprint("%v %v\n", k, cur_k_reduce_result)
					total_reduce_result = append(total_reduce_result, format_reduce_result)
				}

				out_file_name := reduceResultWriter(total_reduce_result, req_reply.ReduceID)

				out_files[0] = out_file_name
			}
			if req_reply.TaskType == Task_Type_Wait {
				fmt.Printf("task type is wait\n")
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

func mapResultWriter(map_result []KeyValue, task_ID int64) *map[int]string {
	split_map_result := make(map[int][]KeyValue)

	for _, v := range map_result {
		k := v.Key
		reduce_ID := ihash(k) % 6
		_, ok := split_map_result[reduce_ID]
		if !ok {
			split_map_result[reduce_ID] = make([]KeyValue, 0)
		}
		split_map_result[reduce_ID] = append(split_map_result[reduce_ID], v)
	}

	output_files_name := make(map[int]string)
	for reduce_ID, data := range split_map_result {
		cur_ID_output := fmt.Sprintf("./int/mr-int-%v-%v", task_ID, reduce_ID)
		output_files_name[reduce_ID] = cur_ID_output

		ofile, err := os.Create(cur_ID_output)
		//fmt.Printf("key: %v, value: %v\n", reduce_ID, data)
		if err != nil {
			log.Fatal(err.Error())
		}

		enc := json.NewEncoder(ofile)
		for _, v := range data {
			//fmt.Printf("%v:%v\n", v.Key, v.Value)
			err := enc.Encode(&v)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}

	return &output_files_name
}

func parseMapResult(input_files []string) map[string][]string {
	input_data := make([]KeyValue, 0)
	for _, file := range input_files {
		cur_input, err := os.Open(file)
		if err != nil {
			fmt.Printf("open file error with %v", file)
		}

		dec := json.NewDecoder(cur_input)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			input_data = append(input_data, kv)
		}
	}

	return nil
}

func reduceResultWriter(reduce_result []string, reduce_ID int) string {
	out_file_name := "mr-out-" + string(reduce_ID)
	out_file, err := os.Create(out_file_name)
	if err != nil {
		fmt.Printf("output No.%v reduce result failed!", reduce_ID)
		return ""
	}

	for one_line := range reduce_result {
		fmt.Fprint(out_file, one_line)
	}

	return out_file_name
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
