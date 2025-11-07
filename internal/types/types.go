package types

// KeyValue is the intermediate key-value pair produced by mappers.
type KeyValue struct {
	Key   string
	Value string
}

// Task represents a single map or reduce task.
type Task struct {
	Type      string // "map" or "reduce"
	ID        int
	InputFile string
	Output    string
}

// TaskResult represents the result of a task execution.
type TaskResult struct {
	TaskID int
	Status string // "success" or "failed"
	Output interface{}
}
