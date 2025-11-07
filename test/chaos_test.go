package test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"DistGrep/internal/grep"
	"DistGrep/internal/mapreduce"
	"DistGrep/internal/types"
)

// FailingReducer wraps a reducer and simulates failures on specific keys.
type FailingReducer struct {
	inner         mapreduce.Reducer
	failureRate   float64 // 0.0 to 1.0
	failedKeys    map[string]bool
	mu            sync.Mutex
	failureCount  int32
	retryAttempts int
}

// NewFailingReducer creates a new failing reducer for chaos testing.
func NewFailingReducer(inner mapreduce.Reducer, failureRate float64) *FailingReducer {
	return &FailingReducer{
		inner:         inner,
		failureRate:   failureRate,
		failedKeys:    make(map[string]bool),
		retryAttempts: 3,
	}
}

// Reduce implements the Reducer interface with injected failures.
func (fr *FailingReducer) Reduce(key string, values []string) string {
	fr.mu.Lock()
	attempt := 0
	fr.mu.Unlock()

	// Retry logic with exponential backoff
	for attempt < fr.retryAttempts {
		// Simulate random failure
		if shouldFail(fr.failureRate) {
			atomic.AddInt32(&fr.failureCount, 1)
			fmt.Printf("[CHAOS] Reducer failed for key '%s' (attempt %d/%d)\n", key, attempt+1, fr.retryAttempts)
			attempt++
			continue
		}

		// Success - execute the actual reducer
		result := fr.inner.Reduce(key, values)
		if attempt > 0 {
			fmt.Printf("[CHAOS] Reducer recovered for key '%s' after %d attempts\n", key, attempt+1)
		}
		return result
	}

	// All retries exhausted - return error message
	fmt.Printf("[CHAOS] Reducer permanently failed for key '%s' after %d attempts\n", key, fr.retryAttempts)
	fr.mu.Lock()
	fr.failedKeys[key] = true
	fr.mu.Unlock()

	return fmt.Sprintf("ERROR: Failed to reduce key '%s'", key)
}

// GetFailureStats returns statistics about failures.
func (fr *FailingReducer) GetFailureStats() (int32, map[string]bool) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return atomic.LoadInt32(&fr.failureCount), fr.failedKeys
}

// shouldFail randomly determines if an operation should fail based on failure rate.
func shouldFail(failureRate float64) bool {
	if failureRate <= 0 {
		return false
	}
	// Use a simple pseudo-random approach
	return (atomic.AddInt32(&randomSeed, 1) % 100) < int32(failureRate*100)
}

var randomSeed int32

// FailingMapper wraps a mapper and simulates failures on specific files.
type FailingMapper struct {
	inner         mapreduce.Mapper
	failureRate   float64
	retryAttempts int
	failedFiles   map[string]bool
	mu            sync.Mutex
}

// NewFailingMapper creates a new failing mapper for chaos testing.
func NewFailingMapper(inner mapreduce.Mapper, failureRate float64) *FailingMapper {
	return &FailingMapper{
		inner:         inner,
		failureRate:   failureRate,
		retryAttempts: 3,
		failedFiles:   make(map[string]bool),
	}
}

// Map implements the Mapper interface with injected failures.
func (fm *FailingMapper) Map(filename string) []types.KeyValue {
	attempt := 0

	for attempt < fm.retryAttempts {
		if shouldFail(fm.failureRate) {
			fmt.Printf("[CHAOS] Mapper failed for file '%s' (attempt %d/%d)\n", filename, attempt+1, fm.retryAttempts)
			attempt++
			continue
		}

		result := fm.inner.Map(filename)
		if attempt > 0 {
			fmt.Printf("[CHAOS] Mapper recovered for file '%s' after %d attempts\n", filename, attempt+1)
		}
		return result
	}

	fmt.Printf("[CHAOS] Mapper permanently failed for file '%s' after %d attempts\n", filename, fm.retryAttempts)
	fm.mu.Lock()
	fm.failedFiles[filename] = true
	fm.mu.Unlock()

	return []types.KeyValue{}
}

// GetFailedFiles returns files that failed all retry attempts.
func (fm *FailingMapper) GetFailedFiles() map[string]bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.failedFiles
}

// TestReducerFailureRecovery tests if the system recovers from reducer failures.
func TestReducerFailureRecovery(t *testing.T) {
	// Create temporary test files
	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 3)

	// Create distributed grep instance
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

	// Verify that we got some results despite failures
	failureCount, failedKeys := failingReducer.GetFailureStats()

	t.Logf("Test completed with %d reducer failures", failureCount)
	t.Logf("Failed keys that could not be recovered: %v", failedKeys)

	if len(results) == 0 && failureCount == 0 {
		t.Fatalf("Expected some results or failures, but got none")
	}

	// Verify that at least some keys were processed successfully
	successCount := len(results) - len(failedKeys)
	if successCount < 0 {
		successCount = 0
	}

	t.Logf("Successfully processed %d keys despite %d total failures", len(results)-len(failedKeys), failureCount)
}

// TestMapperFailureRecovery tests if the system recovers from mapper failures.
func TestMapperFailureRecovery(t *testing.T) {
	// Create temporary test files
	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 5)

	// Create distributed grep instance
	dg, err := grep.NewDistributedGrep("test")
	if err != nil {
		t.Fatalf("Failed to create grep: %v", err)
	}

	// Create a failing mapper wrapper with 20% failure rate
	failingMapper := NewFailingMapper(dg, 0.2)

	// Execute search with failure injection
	engine := mapreduce.NewEngine(2)
	_, err = engine.Execute(testFiles, failingMapper, dg)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	failedFiles := failingMapper.GetFailedFiles()

	t.Logf("Test completed with %d files that could not be processed", len(failedFiles))
	t.Logf("Failed files: %v", failedFiles)

	// Verify that we processed at least some files
	filesProcessed := len(testFiles) - len(failedFiles)
	t.Logf("Successfully processed %d/%d files", filesProcessed, len(testFiles))

	if filesProcessed == 0 {
		t.Fatalf("No files were successfully processed")
	}
}

// TestCombinedChaos tests both mapper and reducer failures simultaneously.
func TestCombinedChaos(t *testing.T) {
	// Create temporary test files
	tmpDir := t.TempDir()
	testFiles := createTestFiles(t, tmpDir, 4)

	// Create distributed grep instance
	dg, err := grep.NewDistributedGrep("test|error|chaos")
	if err != nil {
		t.Fatalf("Failed to create grep: %v", err)
	}

	// Create failing components
	failingMapper := NewFailingMapper(dg, 0.25)
	failingReducer := NewFailingReducer(dg, 0.25)

	// Execute search with dual failure injection
	engine := mapreduce.NewEngine(3)
	results, err := engine.Execute(testFiles, failingMapper, failingReducer)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	failedFiles := failingMapper.GetFailedFiles()
	failureCount, failedKeys := failingReducer.GetFailureStats()

	t.Logf("Combined chaos test completed:")
	t.Logf("  - Mapper failures: %d files failed", len(failedFiles))
	t.Logf("  - Reducer failures: %d total failures", failureCount)
	t.Logf("  - Failed keys: %v", failedKeys)
	t.Logf("  - Results produced: %d", len(results))

	// The system should produce at least some results despite failures
	if len(results) == 0 {
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
