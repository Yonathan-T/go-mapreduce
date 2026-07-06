package mapreduce

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

type TaskType int

const (
	TaskMap    TaskType = iota
	TaskReduce
	TaskNone
	TaskExit
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type MapTask struct {
	ID    int
	Input string
}

type ReduceTask struct {
	ID  int
	Key string
}

type Task struct {
	Type   TaskType
	Map    MapTask
	Reduce ReduceTask
}
