package main

import (
	mapreduce "MapReduce"
	"fmt"
	"strings"
	"time"
)

func mapFn(docID string, contents string) []mapreduce.KeyValue {
	words := strings.Fields(contents)
	var kvs []mapreduce.KeyValue
	for _, w := range words {
		kvs = append(kvs, mapreduce.KeyValue{Key: strings.ToLower(w), Value: "1"})
	}
	return kvs
}

func reduceFn(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func main() {
	inputs := []string{
		"foo bar baz foo",
		"bar baz qux",
		"foo qux qux qux",
	}
	startTime := time.Now()
	m := mapreduce.NewMaster(inputs, 3, mapFn, reduceFn)
	m.Run(4)
	fmt.Println(m.Output())
	fmt.Println("Time taken:", time.Since(startTime))
}
