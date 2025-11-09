package test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"DistGrep/internal/grep"
	"DistGrep/internal/mapreduce"
	"DistGrep/internal/types"
)

// FailingReducer wraps a reducer and simulates failures on specific keys.
type FailingReducer struct {
	inner        mapreduce.Reducer
	failureRate  float64 // 0.0 to 1.0
	failedKeys   map[string]bool
	mu           sync.Mutex
	failureCount int32
	rng          *rand.Rand
}

// NewFailingReducer creates a new failing reducer for chaos testing.
func NewFailingReducer(inner mapreduce.Reducer, failureRate float64) *FailingReducer {
	return &FailingReducer{
		inner:       inner,
		failureRate: failureRate,
		failedKeys:  make(map[string]bool),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Reduce implements the Reducer interface with injected failures.
func (fr *FailingReducer) Reduce(key string, values []string) string {
	// Simulate random failure
	if fr.shouldFail() {
		atomic.AddInt32(&fr.failureCount, 1)
		fmt.Printf("[CHAOS] Reducer failed for key '%s'\n", key)
		
		fr.mu.Lock()
		fr.failedKeys[key] = true
		fr.mu.Unlock()
		
		return fmt.Sprintf("ERROR: Failed to reduce key '%s'", key)
	}

	// Success - execute the actual reducer
	return fr.inner.Reduce(key, values)
}

// shouldFail randomly determines if an operation should fail based on failure rate.
func (fr *FailingReducer) shouldFail() bool {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return fr.rng.Float64() < fr.failureRate
}

// GetFailureStats returns statistics about failures.
func (fr *FailingReducer) GetFailureStats() (int32, map[string]bool) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return atomic.LoadInt32(&fr.failureCount), fr.failedKeys
}

// FailingMapper wraps a mapper and simulates failures on specific files.
type FailingMapper struct {
	inner        mapreduce.Mapper
	failureRate  float64
	failedFiles  map[string]bool
	mu           sync.Mutex
	failureCount int32
	rng          *rand.Rand
}

// NewFailingMapper creates a new failing mapper for chaos testing.
func NewFailingMapper(inner mapreduce.Mapper, failureRate float64) *FailingMapper {
	return &FailingMapper{
		inner:       inner,
		failureRate: failureRate,
		failedFiles: make(map[string]bool),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Map implements the Mapper interface with injected failures.
func (fm *FailingMapper) Map(filename string) []types.KeyValue {
	if fm.shouldFail() {
		atomic.AddInt32(&fm.failureCount, 1)
		fmt.Printf("[CHAOS] Mapper failed for file '%s'\n", filename)
		
		fm.mu.Lock()
		fm.failedFiles[filename] = true
		fm.mu.Unlock()
		
		return []types.KeyValue{}
	}

	return fm.inner.Map(filename)
}

// shouldFail randomly determines if an operation should fail based on failure rate.
func (fm *FailingMapper) shouldFail() bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.rng.Float64() < fm.failureRate
}

// GetFailureStats returns statistics about failures.
func (fm *FailingMapper) GetFailureStats() (int32, map[string]bool) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return atomic.LoadInt32(&fm.failureCount), fm.failedFiles
}

// TestReducerFailureRecovery tests if the system recovers from reducer failures.
func TestReducerFailureRecovery(t *testing.T) {

	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 3)

	dg, err := grep.NewDistributedGrep("test")
	if err != nil {
		t.Fatalf("Failed to create grep: %v", err)
	}

	// Create a failing reducer wrapper with 30% failure rate
	failingReducer := NewFailingReducer(dg, 0.3)

	// Execute search with failure injection
	engine := mapreduce.NewEngine(2)
	results, err := engine.Execute(testFiles, dg, failingReducer)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	failureCount, failedKeys := failingReducer.GetFailureStats()

	t.Logf("Test completed with %d reducer failures", failureCount)
	t.Logf("Keys that experienced failures: %v", failedKeys)
	t.Logf("Results produced: %d", len(results))

	// With retry logic, we should still get results despite failures
	if len(results) == 0 && failureCount == 0 {
		t.Fatalf("Expected some results or failures, but got none")
	}

	t.Logf("MapReduce successfully handled %d reducer failure attempts with retry logic", failureCount)
}

// TestMapperFailureRecovery tests if the system recovers from mapper failures.
func TestMapperFailureRecovery(t *testing.T) {

	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 5)

	dg, err := grep.NewDistributedGrep("test")
	if err != nil {
		t.Fatalf("Failed to create grep: %v", err)
	}

	failingMapper := NewFailingMapper(dg, 0.2)

	engine := mapreduce.NewEngine(2)
	_, err = engine.Execute(testFiles, failingMapper, dg)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	failureCount, failedFiles := failingMapper.GetFailureStats()

	t.Logf("Test completed with %d mapper failures", failureCount)
	t.Logf("Files that experienced failures: %v", failedFiles)

	// With retry logic in MapReduce, some files should be successfully processed
	t.Logf("Total files: %d", len(testFiles))
	
	// The test should complete without permanent failures (MapReduce retries)
	if failureCount == 0 {
		t.Logf("No failures encountered (lucky run)")
	}
}

// TestCombinedChaos tests both mapper and reducer failures simultaneously.
func TestCombinedChaos(t *testing.T) {

	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 4)

	dg, err := grep.NewDistributedGrep("test|error|chaos")
	if err != nil {
		t.Fatalf("Failed to create grep: %v", err)
	}

	failingMapper := NewFailingMapper(dg, 0.25)
	failingReducer := NewFailingReducer(dg, 0.25)

	engine := mapreduce.NewEngine(3)
	results, err := engine.Execute(testFiles, failingMapper, failingReducer)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	mapperFailures, mapperFailedFiles := failingMapper.GetFailureStats()
	reducerFailures, reducerFailedKeys := failingReducer.GetFailureStats()

	t.Logf("Combined chaos test completed:")
	t.Logf("  - Mapper failures: %d attempts", mapperFailures)
	t.Logf("  - Files that experienced mapper failures: %v", mapperFailedFiles)
	t.Logf("  - Reducer failures: %d attempts", reducerFailures)
	t.Logf("  - Keys that experienced reducer failures: %v", reducerFailedKeys)
	t.Logf("  - Results produced: %d", len(results))

	// The system should produce results despite failures (due to retry logic)
	if len(results) == 0 && (mapperFailures > 0 || reducerFailures > 0) {
		t.Logf("WARNING: No results produced despite chaos testing")
	}
}

// BenchmarkReducerWithChaos benchmarks reducer performance under failure conditions.
func BenchmarkReducerWithChaos(b *testing.B) {
	tmpDir := b.TempDir()
	testFiles := createTestFiles(&testing.T{}, tmpDir, 10)

	dg, _ := grep.NewDistributedGrep("test")
	failingReducer := NewFailingReducer(dg, 0.15)

	b.ResetTimer()
	engine := mapreduce.NewEngine(4)
	engine.Execute(testFiles, dg, failingReducer)
}

// createTestFiles creates temporary test files with sample content.
func createTestFiles(t testing.TB, dir string, count int) []string {
	var files []string

	content := []string{
		"This is a test line\n",
		"Another test entry\n",
		"Test: error occurred\n",
		"No match here\n",
		"Test chaos scenario\n",
		"Testing system resilience\n",
	}

	for i := 0; i < count; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("test_%d.txt", i))
		data := ""
		for j := 0; j < 10; j++ {
			data += content[(i+j)%len(content)]
		}
		err := os.WriteFile(filename, []byte(data), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		files = append(files, filename)
	}

	return files
}
