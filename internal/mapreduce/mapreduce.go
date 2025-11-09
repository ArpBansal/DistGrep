package mapreduce

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"DistGrep/internal/types"
)

// Mapper defines the map function interface.
type Mapper interface {
	Map(filename string) []types.KeyValue
}

// Reducer defines the reduce function interface.
type Reducer interface {
	Reduce(key string, values []string) string
}

// Engine is the MapReduce execution engine.
type Engine struct {
	numReducers int
	mu          sync.Mutex
}

// NewEngine creates a new MapReduce engine.
func NewEngine(numReducers int) *Engine {
	return &Engine{
		numReducers: numReducers,
	}
}

// Execute runs the MapReduce job on the given input files.
func (e *Engine) Execute(
	files []string,
	mapper Mapper,
	reducer Reducer,
) (map[string]string, error) {
	intermediates := e.mapPhase(files, mapper)

	shuffled := e.shuffle(intermediates)

	result := e.reducePhase(shuffled, reducer)

	return result, nil
}

// mapPhase executes the map phase in parallel.
func (e *Engine) mapPhase(files []string, mapper Mapper) []types.KeyValue {
	var wg sync.WaitGroup
	var mu sync.Mutex
	intermediates := []types.KeyValue{}

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			kvs := mapper.Map(f)
			mu.Lock()
			intermediates = append(intermediates, kvs...)
			mu.Unlock()
		}(file)
	}

	wg.Wait()
	return intermediates
}

// shuffle groups key-value pairs by key and sorts values.
func (e *Engine) shuffle(kvs []types.KeyValue) map[string][]string {
	grouped := make(map[string][]string)

	for _, kv := range kvs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	for key := range grouped {
		sort.Strings(grouped[key])
	}

	return grouped
}

// reducePhase executes the reduce phase in parallel.
func (e *Engine) reducePhase(
	shuffled map[string][]string,
	reducer Reducer,
) map[string]string {
	var wg sync.WaitGroup
	var mu sync.Mutex
	result := make(map[string]string)

	keys := make([]string, 0, len(shuffled))
	for k := range shuffled {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sem := make(chan struct{}, e.numReducers)

	for _, key := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			output := reducer.Reduce(k, shuffled[k])
			mu.Lock()
			result[k] = output
			mu.Unlock()
		}(key)
	}

	wg.Wait()
	return result
}

// WriteOutput writes the result to output files.
func WriteOutput(result map[string]string, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	for key, value := range result {
		filename := fmt.Sprintf("%s/%s.txt", outputDir, key)
		if err := os.WriteFile(filename, []byte(value), 0644); err != nil {
			return fmt.Errorf("failed to write output file %s: %w", filename, err)
		}
	}

	return nil
}
