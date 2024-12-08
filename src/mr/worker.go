package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	args := AssignTaskArgs{rand.Int()}
	// Your worker implementation here.
	for {

		reply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			log.Fatalf("Worker: failed to call Coordinator.AssignTask")
		}
		if reply.TaskType == MapStage {
			// Todo:处理 Map 任务
			doMapTask(args.WorkerId, mapf, reply)
		} else if reply.TaskType == ReduceStage {
			//Todo: 处理 Reduce 任务
			doReduceTask(args.WorkerId, reducef, reply)
		} else if reply.TaskType == Waiting {
			log.Println("Worker: no task assigned")
			time.Sleep(1 * time.Second)
		} else {
			return
		}
		time.Sleep(3 * time.Second)
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func doMapTask(workerId int, mapf func(string, string) []KeyValue, task Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		return
	}
	file.Close()
	nReduce := task.ReduceNumber
	kva := mapf(task.FileName, string(content))
	HashKv := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		mapId := ihash(kv.Key) % nReduce
		HashKv[mapId] = append(HashKv[mapId], kv)
	}

	for i := 0; i < nReduce; i++ {
		tmpfile, err := os.CreateTemp("", "tmp-*")
		if err != nil {
			log.Fatalf("cannot create tmp file on worker %v,error:%v", workerId, err)
			return
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range HashKv[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encoder kv to json %v,error:%v", workerId, err)
				return
			}
		}
		err = os.Rename(tmpfile.Name(), "mr-"+strconv.Itoa(task.TaskId)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("Unable to convert temporary file to permanent file,workerId: %v,error:%v", workerId, err)
			return
		}
		tmpfile.Close()
	}

	log.Printf("worker:%d finish map task:%d", workerId, task.TaskId)

	CallFinishTask(&TaskFinishArgs{
		WorkerId: workerId,
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	})

}

func findReduceFiles(reduceID int) ([]string, error) {
	// 构造正则表达式：mr-*-reduceid
	pattern := fmt.Sprintf(`^mr-.*-%d$`, reduceID)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// 获取当前目录
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// 用来存储匹配的文件
	var matchedFiles []string

	// 遍历目录中的文件
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 检查文件名是否匹配正则表达式
		if !info.IsDir() && regex.MatchString(info.Name()) {
			matchedFiles = append(matchedFiles, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return matchedFiles, nil
}

func doReduceTask(workerId int, reducef func(string, []string) string, task Task) {

	filenames, err := findReduceFiles(task.TaskId)
	if err != nil {
		log.Fatalf("fail to search imtermediate file")
		return
	}

	// for _, filename := range filenames {
	// 	log.Printf("workder: %d find matched file %s", workerId, filename)
	// }
	var kva []KeyValue
	for _, filename := range filenames {
		file, err0 := os.Open(filename)
		if err0 != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))
	tmpfile, _ := os.CreateTemp("", "tmp-"+strconv.Itoa(workerId))

	//ofile, _ := os.Create(tmpfile)
	// log.Printf("total k-v pairs number:%d", len(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	tmpfile.Close()
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	os.Rename(tmpfile.Name(), oname)

	log.Printf("worker:%d finish reduce task:%d", workerId, task.TaskId)
	CallFinishTask(&TaskFinishArgs{
		WorkerId: workerId,
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	})
}

func CallFinishTask(args *TaskFinishArgs) {
	reply := TaskFinishReplys{}
	call("Coordinator.TaskFinish", args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
