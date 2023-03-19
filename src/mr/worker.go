package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

//参考mrsequential.go中的实现排序类型ByKey
//for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	// 每一个worker循环执行以下逻辑
	for {
		// 先取任务
		args := TaskRequest{}
		reply := TaskResponse{}
		CallGetTask(&args, &reply)
		state := reply.State
		//根据返回任务的不同状态，分别处理
		if state == 0 {
			id := strconv.Itoa(reply.XTask.IdMap)
			fileName := reply.XTask.FileName
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open mapTask %s", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %s", fileName)
			}
			file.Close()
			// 解析mrapp/wc.go中的map方法，将content字符串切分成key/value键值对数组，key为拆分出的每个关键字，value为1（这里fileName没什么用）
			kva := mapf(fileName, string(content)) // 接下来要把kva写到中间文件中去
			// 生成与reduce任务个数相同的中间文件
			numReduce := reply.NumReduceTask
			bucket := make([][]KeyValue, numReduce)
			//接下来是论文Figure1的local write阶段, 即将读入的文件内容，按照hash值分类保存到bucket中
			for _, kv := range kva {
				num := ihash(kv.Key) % numReduce
				bucket[num] = append(bucket[num], kv) // key的hash值为num的kv，分到了bucket[num]
			}
			//分完桶后根据reduce的个数写入临时文件tmpFile
			for i := 0; i < numReduce; i++ {
				tmpFile, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					fmt.Printf("error is : %+v\n", error)
					log.Fatal("cannot open map tmpFile")
				}
				//参考课程提示,用json向tmpFile中写bucket
				//enc为 *json.Encoder格式，代表以json格式编码往tmpFile文件中写
				enc := json.NewEncoder(tmpFile)
				// 把bucket[i]的内容传递给enc
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatal("encod bucket error")
				}
				tmpFile.Close()
				//根据课程提示，把中间文件rename
				outFileName := `mr-` + id + `-` + strconv.Itoa(i)
				os.Rename(tmpFile.Name(), outFileName)
			}
			//map任务完成后向MapTaskFin中发送一个true
			CallTaskFin()
		} else if state == 1 {
			//否则就是reduce
			//每一个reduce的任务取numMap个中间文件，取对应的最后一个数字为自己task id的中间文件，因为中间文件的名字最后一个数字是取哈希值得到的
			numMap := reply.NumMapTask
			id := strconv.Itoa(reply.XTask.IdReduce)
			//说明所有的map任务完成，开始reduce
			//kva保存从中间文件中读出的key value对
			intermediate := []KeyValue{}
			//reduce开始读中间文件
			for i := 0; i < numMap; i++ {
				mapFileName := "mr-" + strconv.Itoa(i) + "-" + id
				// inputFile为 *os.File格式，读中间文件
				inputFile, err := os.OpenFile(mapFileName, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %s\n", mapFileName)
				}
				// 将inputFile按照json格式解析
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					//以上把中间文件中读出的键值对保存到了intermediate中
					intermediate = append(intermediate, kv...)
				}
				//将intermediate强转为ByKey类型，排序
				sort.Sort(ByKey(intermediate))

				//准备整合，去重

				//创建一个tmpFile,存放reduce输出结果
				outFileName := "mr-out-" + id
				tmpFile, err := ioutil.TempFile("", "mr-reduce-*")
				if err != nil {
					log.Fatalf("cannot open reduce tmpFile")
				}

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					//values累积了所有key相同的kv对的value值，单词例子中为很多个1
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					// reducef为插件解析的mrapp/wc.go中的reduce方法，返回values的数组长度，即values有多少个1（这里intermediate[i].Key没有用）
					output := reducef(intermediate[i].Key, values)

					//将每个key和对应的有多少个数按格式拼好，写入到tmpFile
					fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
					//再遍历下一批key相等的键值对
					i = j
				}
				tmpFile.Close()
				//改名字
				os.Rename(tmpFile.Name(), outFileName)
			}
			CallTaskFin()
		} else {
			// state == 2的情况
			break
		}
	}

}

//参考下面的CallExample方法实现获取任务的方法
func CallGetTask(args *TaskRequest, reply *TaskResponse) {
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("CallGetTask method - reply.FileName %s\n", reply.XTask.FileName)
	} else {
		fmt.Printf("CallGetTask method failed!\n")
	}
}

func CallTaskFin() {
	// 这两个参数没有用，但是call方法必须要给参数
	args := ExampleArgs{}
	reply := ExampleReply{}
	ok := call("Coordinator.TaskFin", &args, &reply)
	if ok {
		fmt.Printf("CallTaskFin method ok\n")
	} else {
		fmt.Printf("CallTaskFin method failed\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
