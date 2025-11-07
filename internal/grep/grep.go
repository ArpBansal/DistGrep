package grep

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"DistGrep/internal/mapreduce"
	"DistGrep/internal/types"
)

// DistributedGrep handles distributed grep operations using MapReduce.
type DistributedGrep struct {
	pattern string
	regex   *regexp.Regexp
}

// NewDistributedGrep creates a new DistributedGrep instance.
func NewDistributedGrep(pattern string) (*DistributedGrep, error) {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	return &DistributedGrep{
		pattern: pattern,
		regex:   regex,
	}, nil
}

// Map implements the Mapper interface for grep.
// It reads lines from a file and emits (line, filename) for matching lines.
func (dg *DistributedGrep) Map(filename string) []types.KeyValue {
	var results []types.KeyValue

	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open file %s: %v\n", filename, err)
		return results
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if dg.regex.MatchString(line) {
			// Key is the matched line, Value is the filename:line_number
			results = append(results, types.KeyValue{
				Key:   line,
				Value: filename,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanner error on file %s: %v\n", filename, err)
	}

	return results
}

// Reduce implements the Reducer interface for grep.
// It combines all occurrences of a matched line from different files.
func (dg *DistributedGrep) Reduce(key string, values []string) string {
	// key is the matched line
	// values is the list of filenames where this line was found

	if len(values) == 0 {
		return ""
	}

	// Format: matched_line -> [file1, file2, ...]
	filesStr := strings.Join(values, ", ")
	return fmt.Sprintf("%s -> [%s]", key, filesStr)
}

// collectFiles recursively collects all files from paths (files and directories).
func (dg *DistributedGrep) collectFiles(paths []string) ([]string, error) {
	var files []string

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("failed to stat %s: %w", path, err)
		}

		if info.IsDir() {
			// Walk the directory recursively
			err := filepath.Walk(path, func(p string, f os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				// Only process regular files, skip directories and symlinks
				if f.IsDir() {
					return nil
				}
				files = append(files, p)
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to walk directory %s: %w", path, err)
			}
		} else {
			// It's a file
			files = append(files, path)
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files found in the given paths")
	}

	return files, nil
}

// Search performs distributed grep on the given files and directories.
func (dg *DistributedGrep) Search(paths []string, numReducers int) (map[string]string, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no files or directories provided")
	}

	// Collect all files from paths (which may include directories)
	files, err := dg.collectFiles(paths)
	if err != nil {
		return nil, err
	}

	engine := mapreduce.NewEngine(numReducers)
	return engine.Execute(files, dg, dg)
}

// PrintResults prints the grep results to stdout.
func PrintResults(results map[string]string) {
	if len(results) == 0 {
		fmt.Println("No matches found")
		return
	}

	for _, locations := range results {
		fmt.Println(locations)
	}
}
