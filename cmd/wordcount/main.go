package main

import (
	mapreduce "MapReduce"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ledongthuc/pdf"
)

func mapFn(docID string, contents string) []mapreduce.KeyValue {
	words := strings.Fields(contents)
	var kvs []mapreduce.KeyValue
	for _, w := range words {
		w = strings.ToLower(w)
		w = strings.Trim(w, "\"',.;:!?()-[]{}#$%&*@^~<>/\\|=+`")
		if len(w) == 0 {
			continue
		}
		onlyLetters := true
		for _, r := range w {
			if r < 'a' || r > 'z' {
				onlyLetters = false
				break
			}
		}
		if !onlyLetters {
			continue
		}
		kvs = append(kvs, mapreduce.KeyValue{Key: w, Value: "1"})
	}
	return kvs
}

func reduceFn(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func extractText(path string) (string, error) {
	f, r, err := pdf.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var sb strings.Builder
	totalPage := r.NumPage()
	for i := 1; i <= totalPage; i++ {
		p := r.Page(i)
		text, err := p.GetPlainText(nil)
		if err != nil {
			continue
		}
		sb.WriteString(text)
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

func splitIntoChunks(text string, numChunks int) []string {
	if numChunks <= 1 {
		return []string{text}
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{text}
	}

	chunkSize := (len(words) + numChunks - 1) / numChunks
	chunks := make([]string, 0, numChunks)

	for i := 0; i < len(words); i += chunkSize {
		end := i + chunkSize
		if end > len(words) {
			end = len(words)
		}
		chunks = append(chunks, strings.Join(words[i:end], " "))
	}
	return chunks
}

func readFile(path string) (string, error) {
	text, err := extractText(path)
	if err == nil {
		return text, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file.pdf_or_txt> [file2 ...]\n", os.Args[0])
		os.Exit(1)
	}

	var paths []string
	for _, arg := range os.Args[1:] {
		if strings.ContainsAny(arg, "*?") {
			matches, err := filepath.Glob(arg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Bad glob %q: %v\n", arg, err)
				continue
			}
			paths = append(paths, matches...)
		} else {
			paths = append(paths, arg)
		}
	}

	for _, p := range paths {
		ext := strings.ToLower(filepath.Ext(p))
		if ext != ".txt" && ext != ".pdf" {
			fmt.Fprintf(os.Stderr, "Unsupported file type: %s (only .txt and .pdf allowed)\n", p)
			os.Exit(1)
		}
	}

	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "No matching files found")
		os.Exit(1)
	}

	var allText strings.Builder
	for _, p := range paths {
		text, err := readFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", p, err)
			os.Exit(1)
		}
		allText.WriteString(text)
		allText.WriteString("\n")
	}

	fullText := allText.String()
	if len(strings.TrimSpace(fullText)) == 0 {
		fmt.Fprintln(os.Stderr, "No readable input files")
		os.Exit(1)
	}

	inputs := splitIntoChunks(fullText, 8)

	executionTime := time.Now()
	m := mapreduce.NewMaster(inputs, 5, mapFn, reduceFn)
	m.Run(6)
	output := m.Output()
	os.WriteFile("mr-output.txt", []byte(output), 0644)
	elapsed := time.Since(executionTime)
	if elapsed < time.Second {
		fmt.Fprintf(os.Stderr, "Chunks: %d, \033[32mExecution time: %s\033[0m\n", len(inputs), elapsed)
	} else {
		fmt.Fprintf(os.Stderr, "Chunks: %d, \033[31mExecution time: %s\033[0m\n", len(inputs), elapsed)
	}
}
