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
	numReducers  int
	maxRetries   int
	mu           sync.Mutex
}

// NewEngine creates a new MapReduce engine.
func NewEngine(numReducers int) *Engine {
	return &Engine{
		numReducers: numReducers,
		maxRetries:  3, // Default retry count
	}
}

// SetMaxRetries configures the maximum number of retries for failed operations.
func (e *Engine) SetMaxRetries(retries int) {
	e.maxRetries = retries
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

// mapPhase executes the map phase in parallel with retry logic.
func (e *Engine) mapPhase(files []string, mapper Mapper) []types.KeyValue {
	var wg sync.WaitGroup
	var mu sync.Mutex
	intermediates := []types.KeyValue{}

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			
			var kvs []types.KeyValue
			for attempt := 0; attempt < e.maxRetries; attempt++ {
				kvs = mapper.Map(f)
				
				if len(kvs) > 0 {
					break
				}
				
				if attempt == e.maxRetries-1 {
					fmt.Fprintf(os.Stderr, "[MapReduce] Warning: Mapper failed for file '%s' after %d attempts\n", f, e.maxRetries)
				}
			}
			
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

// reducePhase executes the reduce phase in parallel with retry logic.
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

			var output string
			for attempt := 0; attempt < e.maxRetries; attempt++ {
				output = reducer.Reduce(k, shuffled[k])
				
				if output != "" && len(output) > 0 {
					if len(output) >= 6 && output[:6] == "ERROR:" {
						if attempt == e.maxRetries-1 {
							fmt.Fprintf(os.Stderr, "[MapReduce] Warning: Reducer failed for key '%s' after %d attempts\n", k, e.maxRetries)
						}
						continue
					}
					break
				}
				
				if attempt == e.maxRetries-1 {
					fmt.Fprintf(os.Stderr, "[MapReduce] Warning: Reducer returned empty for key '%s' after %d attempts\n", k, e.maxRetries)
				}
			}
			
			if output != "" && !(len(output) >= 6 && output[:6] == "ERROR:") {
				mu.Lock()
				result[k] = output
				mu.Unlock()
			}
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
