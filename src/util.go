package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func split(fileInfo FileInfo, chunkSize int64) (indexList []int64, actualChunkSize int64) {
	partNumber := int64(1)
	indexList = []int64{0}

	if int64(math.Ceil(float64(fileInfo.Size)/float64(chunkSize))) > 10000 {
		chunkSize = fileInfo.Size/10000 + 1024
	}

	for chunkSize*partNumber < fileInfo.Size {
		indexList = append(indexList, chunkSize*partNumber)
		partNumber += 1
	}
	return indexList, chunkSize
}

func withRetries(fn RetryFunc) error {
	var err error
	for i := 0; i < cfg.MaxRetries; i++ {
		err = fn()
		if err == nil {
			break
		}
		log.Println("Failed to execute function: ", err, ". Retrying...")
		time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
	}
	return err
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ByteCountSI(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dBytes", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cBytes", float64(b)/float64(div), "KMGTPE"[exp])
}

func getIgnoreList() []string {
	log.Printf("Checking ignore files list in %s\n", cfg.IgnoreListPath)
	ignoreList := []string{}
	_, err := os.Stat(cfg.IgnoreListPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No ignore list in path %s\n", cfg.IgnoreListPath)
		} else {
			log.Println(err)
		}
		return ignoreList
	}
	file, err := os.Open(cfg.IgnoreListPath)
	if err != nil {
		log.Println(err)
		return ignoreList
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		prefix := strings.TrimPrefix(scanner.Text(), "/")
		ignoreList = append(ignoreList, prefix)
	}
	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
	log.Printf("Found ignore files list with prefix Length: %d, in %s", len(ignoreList), cfg.IgnoreListPath)
	return ignoreList
}

func isIgnored(key string, ignoreList []string) bool {
	for _, prefix := range ignoreList {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

var dlMutex sync.Mutex

func recordDownloadPart(partInfo PartInfo) {
	_ = withRetries(func() error {
		dlMutex.Lock()
		defer dlMutex.Unlock()
		f, err := os.OpenFile(cfg.DBPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = fmt.Fprintf(f, "%s\t%s\t%d\n", partInfo.FromKey, partInfo.FromBucket, partInfo.PartNumber)
		return err
	})
}

func getDownloadedParts(fileInfo FileInfo) ([]int, error) {
	var partnumberList []int
	err := withRetries(func() error {
		f, err := os.Open(cfg.DBPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		defer f.Close()
		partnumberList = []int{}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fields := strings.Split(scanner.Text(), "\t")
			if len(fields) == 3 && fields[0] == fileInfo.FromKey && fields[1] == fileInfo.FromBucket {
				if p, e := strconv.Atoi(fields[2]); e == nil {
					partnumberList = append(partnumberList, p)
				}
			}
		}
		return scanner.Err()
	})
	if err != nil {
		return nil, err
	}
	return partnumberList, nil
}

func deleteDownloadParts(fileInfo FileInfo) error {
	return withRetries(func() error {
		f, err := os.Open(cfg.DBPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		defer f.Close()
		var buf bytes.Buffer
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Split(line, "\t")
			if len(fields) == 3 && fields[0] == fileInfo.FromKey && fields[1] == fileInfo.FromBucket {
				continue
			}
			buf.WriteString(line)
			buf.WriteByte('\n')
		}
		if err := os.WriteFile(cfg.DBPath, buf.Bytes(), 0644); err != nil {
			return err
		}
		return nil
	})
}
