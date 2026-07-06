# go-mapreduce

MapReduce implementation in Go, based on the [Google MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Usage

```
go run cmd/wordcount/main.go <files...>
```

Counts word frequency across one or more `.txt` or `.pdf` files. Supports glob patterns.

```
go run cmd/wordcount/main.go inputs/pg*.txt
go run cmd/wordcount/main.go *.pdf
```

Output is written to `mr-output.txt`.

## Structure

- `types.go` — core types (KeyValue, MapFunc, ReduceFunc, task types)
- `master.go` — Master with concurrent worker pool, task scheduling
- `cmd/wordcount/main.go` — word count example (map/reduce functions + file I/O)

## How it works

1. Master splits input into M map tasks
2. Workers pull tasks and run the map function
3. Map output is grouped by key (shuffle/sort)
4. Workers pull reduce tasks and combine values per key
5. Master writes final output

Concurrent workers (goroutines), mutex-protected shared state, task state tracking (idle → in-progress → completed).
