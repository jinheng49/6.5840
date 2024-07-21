package mr

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int
	Status         TaskStatus
	TaskType       TypeTask
	TaskList       []*BaseTask
	GetTaskChan    chan TaskChan
	ReportTaskChan chan TaskChan
	doneChan       chan struct{}
}

type TaskChan struct {
	TaskInfo *TaskInfo
	OK       chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) handleTask() {
	doneFlag := false
	for {
		select {
		case msg := <-c.GetTaskChan:
			baseTask := c.getTaskOne()
			myLog("coordinator send task to info%+v\n", baseTask)
			*msg.TaskInfo = baseTask.TaskInfo
			msg.OK <- struct{}{}
			if baseTask.Status == StatusDone && !doneFlag {
				c.doneChan <- struct{}{}
				doneFlag = true
			}
		case msg := <-c.ReportTaskChan:
			taskInfo := msg.TaskInfo
			myLog("coordinator receive task report from worker%+v\n", taskInfo)
			c.handleTaskReport(taskInfo)
			msg.OK <- struct{}{}
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	select {
	case <-c.doneChan:
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.init(files, nReduce)
	c.server()
	return &c
}

func (c *Coordinator) init(files []string, nReduce int) {
	myLog("init coordinator start")
	c.nReduce = nReduce
	c.Status = StatusDoing
	c.TaskType = TaskMap
	c.GetTaskChan = make(chan TaskChan)
	c.ReportTaskChan = make(chan TaskChan)
	c.doneChan = make(chan struct{})
	c.initMapTasks(files)
	go c.handleTask()
	myLog("init coordinator end")
}

func (c *Coordinator) GetTask(args string, reply *TaskInfo) error {
	info := TaskChan{
		TaskInfo: reply,
		OK:       make(chan struct{}),
	}
	c.GetTaskChan <- info
	<-info.OK
	return nil
}

func (c *Coordinator) TaskReport(args *TaskInfo, reply *TaskInfo) error {
	info := TaskChan{
		TaskInfo: args,
		OK:       make(chan struct{}),
	}
	c.ReportTaskChan <- info
	<-info.OK
	return nil
}

func (c *Coordinator) getTaskOne() *BaseTask {
	task := c.getMapTask()
	if task != nil {
		tmpTask := *task
		return &tmpTask
	}
	task = c.getReduceTask()
	if task != nil {
		tmpTask := *task
		return &tmpTask
	}
	c.taskDone()
	return &BaseTask{
		Status:   StatusDone,
		TaskInfo: TaskInfo{TaskType: TaskNone},
	}
}
func (c *Coordinator) getSleepTask() *BaseTask {
	return &BaseTask{
		StartTime: time.Time{},
		Status:    "",
		TaskInfo: TaskInfo{
			MapTaskFilePath: "",
			TaskType:        TaskSleep,
			Number:          0,
			NReduce:         0,
		},
	}
}
func (c *Coordinator) getMapTask() *BaseTask {
	if c.TaskType != TaskMap {
		myLog("task type is reduce %s\n", c.string())
		return nil
	}
	taskDoneFlag := true
	for _, task := range c.TaskList {
		if task.judgeAssigned() {
			task.Status = StatusDoing
			task.StartTime = time.Now()
			myLog("get a map task%+v\n", task)
			tmpTask := *task
			return &tmpTask
		}
		taskDoneFlag = taskDoneFlag && task.judgeDone()
	}
	if taskDoneFlag {
		myLog("can not get a map task")
		c.TaskType = TaskReduce
		c.initReduceTasks()
		return nil
	} else {
		myLog("map task need to be waited")
		return c.getSleepTask()
	}
}
func (c *Coordinator) getReduceTask() *BaseTask {
	if c.TaskType != TaskReduce {
		myLog("task type is not reduce but call getReduceTask %s\n", c.string())
		// return nil
	}
	reduceDoneFlag := true
	for _, task := range c.TaskList {
		if task.judgeAssigned() {
			task.Status = StatusDoing
			task.StartTime = time.Now()
			return task
		}
		reduceDoneFlag = reduceDoneFlag && task.judgeDone()
	}
	if reduceDoneFlag {
		c.Status = StatusDone
		myLog("all reduce tasks are done")
		return nil
	} else {
		myLog("reduce task need to be waited")
		return c.getSleepTask()
	}
}
func (c *Coordinator) taskDone() {
	c.Status = StatusDone
	lo.ForEach(c.TaskList, func(baseTask *BaseTask, index int) {
		lo.ForEach(baseTask.ReduceTaskFilePath, func(filePath string, index int) {
			os.Remove(filePath)
		})
	})
}
func (c *Coordinator) handleTaskReport(taskInfo *TaskInfo) {
	switch taskInfo.TaskType {
	case TaskMap:
		c.handleMapTaskReport(taskInfo)
	case TaskReduce:
		c.handleReduceTaskReport(taskInfo)
	}
}
func (c *Coordinator) handleMapTaskReport(taskReported *TaskInfo) {
	for _, task := range c.TaskList {
		if task.judgeSameTask(taskReported) {
			task.Status = StatusDone
			task.TaskInfo = *taskReported
		}
	}
}
func (c *Coordinator) handleReduceTaskReport(taskReported *TaskInfo) {
	for _, task := range c.TaskList {
		if task.judgeSameTask(taskReported) {
			task.Status = StatusDone
			task.TaskInfo = *taskReported
		}
	}
}
func (c *Coordinator) initMapTasks(files []string) {
	myLog("init map tasks start")
	for i, file := range files {
		baseTask := &BaseTask{
			Status: StatusToDo,
			TaskInfo: TaskInfo{
				MapTaskFilePath: file,
				TaskType:        TaskMap,
				Number:          i,
				NReduce:         c.nReduce,
			},
		}
		c.TaskList = append(c.TaskList, baseTask)
		myLog("add map task%+v\n", baseTask)
	}
	myLog("init map tasks end")
}
func (c *Coordinator) initReduceTasks() {
	myLog("init reduce tasks start")
	var reduceTaskList []*BaseTask
	var mapOutputFiles []string
	for _, baseTask := range c.TaskList {
		mapOutputFiles = append(mapOutputFiles, baseTask.ReduceTaskFilePath...)
	}
	for i := 0; i < c.nReduce; i++ {
		reduceTaskList = append(reduceTaskList, NewReduceTask(lo.Filter(mapOutputFiles, func(item string, index int) bool {
			return strings.HasSuffix(item, fmt.Sprintf("-%d", i))
		}), c.nReduce, i))
	}
	c.TaskList = reduceTaskList
	myLog("init reduce tasks end")
}

func (c *Coordinator) string() string {
	buffer := bytes.Buffer{}
	buffer.WriteString("Coordinator: {")
	buffer.WriteString(fmt.Sprintf("Status:%s ", c.Status))
	buffer.WriteString(fmt.Sprintf("TaskType:%s ", c.TaskType))
	buffer.WriteString("TaskList:\n")
	lo.ForEach(c.TaskList, func(item *BaseTask, index int) {
		buffer.WriteString("\t" + item.string() + "\n")
	})
	buffer.WriteString("}")
	return buffer.String()
}
