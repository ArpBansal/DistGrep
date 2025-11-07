package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"DistGrep/internal/grep"
)

func main() {
	pattern := flag.String("p", "", "Pattern to search for (regex)")
	reducers := flag.Int("r", 4, "Number of reducers")
	flag.Parse()

	if *pattern == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -p <pattern> [files...]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	files := flag.Args()
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no input files provided\n")
		os.Exit(1)
	}

	// Create distributed grep instance
	dg, err := grep.NewDistributedGrep(*pattern)
	if err != nil {
		log.Fatalf("Failed to create grep: %v", err)
	}

	// Perform distributed search
	results, err := dg.Search(files, *reducers)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	// Print results
	grep.PrintResults(results)
}
