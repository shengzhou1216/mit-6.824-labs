package mr

import (
	"encoding/json"
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	mapTaskReply, err := RequestMapTask()
	if err != nil {
		fmt.Printf("Request map task err: %s\n", err)
		return
	}
	fmt.Printf("Map task reply: %v\n", mapTaskReply)
	// read file content
	content, err := os.ReadFile(mapTaskReply.File)
	if err != nil {
		fmt.Printf("Read map task's file err: %s\n", err)
		return
	}
	// map file
	keyValues := mapf(mapTaskReply.File, string(content))

	//store intermediate k/vs
	x := mapTaskReply.TaskNumber
	reduceTasks := map[int][]KeyValue{}
	for _, kv := range keyValues {
		y := ihash(kv.Key) % mapTaskReply.NReduce
		reduceTasks[y] = append(reduceTasks[y], kv)
	}
	var reduceFiles []string
	for y, kvs := range reduceTasks {
		file, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			fmt.Printf("Create intermediate file err: %s\n", err)
			return
		}
		enc := json.NewEncoder(file)
		if err := enc.Encode(kvs); err != nil {
			fmt.Printf("Write intermediate file err: %s\n", err)
			return
		}
		file.Close()
		reduceFile := fmt.Sprintf("mr-%d-%d", x, y)
		if err := os.Rename(file.Name(), reduceFile); err != nil {
			fmt.Printf("Rename intermediate file err: %s\n", err)
			return
		}
		reduceFiles = append(reduceFiles, reduceFile)
	}
	// pass back map result to coordinator
	result, err := SubmitMapResult(mapTaskReply.TaskNumber, reduceFiles)
	if err != nil {
		fmt.Printf("Submit map result err: %s\n", err)
		return
	}
	fmt.Println(result)
}

// RequestMapTask request for map task
func RequestMapTask() (MapTaskReply, error) {
	args := MapTaskArgs{}
	reply := MapTaskReply{}
	err := call("Coordinator.RequestMapTask", &args, &reply)
	return reply, err
}

// SubmitMapResult pass back map phase's result to coordinator
func SubmitMapResult(mapTaskNumber int, files []string) (MapResultReply, error) {
	args := MapResultArgs{
		Files:      files,
		TaskNumber: mapTaskNumber,
	}
	reply := MapResultReply{}
	err := call("Coordinator.SubmitMapResult", &args, &reply)
	return reply, err
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
