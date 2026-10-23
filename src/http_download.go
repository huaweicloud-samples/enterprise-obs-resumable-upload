package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

func startHttpDownload(from, to BInfo) error {
	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers))
	var httpList []string

	switch cfg.WorkMode {
	case "HTTP_DOWNLOAD":
		httpList = append(httpList, from.url)
	case "HTTP_DOWNLOAD_LIST":
		file, err := os.Open(from.url)
		if err != nil {
			log.Println("Failed to open file of HTTP_DOWNLOAD_LIST", err)
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			httpList = append(httpList, scanner.Text())
		}
		log.Println("Read local file of HTTP_DOWNLOAD_LIST, total:", len(httpList))
	}

	for _, thisUrl := range httpList {
		semFile.Acquire(context.Background(), 1)
		atomic.AddInt32(&runningGoroutines, 1)
		wg.Add(1)
		go downloadHTTPFile(thisUrl, &wg, semFile)
	}
	wg.Wait()
	return nil
}

func downloadHTTPFile(thisUrl string, wg *sync.WaitGroup, semFile *semaphore.Weighted) error {
	defer wg.Done()
	defer semFile.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)

	URL, err := url.Parse(thisUrl)
	if err != nil {
		log.Printf("Invalid HTTP URL: %s, %v\n", thisUrl, err)
		return err
	}
	from.bucket = strings.Split(URL.Host, ".")[0]
	fullPrefix := strings.TrimSuffix(strings.TrimPrefix(URL.Path, "/"), "/")
	fileName := filepath.Base(fullPrefix)
	localPath := filepath.Join(to.url, fileName)

	fileSize, err := getHTTPFileSize(thisUrl)
	if err != nil {
		log.Println("Failed to get file size:", thisUrl, err)
		return err
	}

	info, err := os.Stat(localPath)
	if !cfg.SkipCompare {
		if err == nil && info.Size() == fileSize {
			log.Println("...File exists and same size, skipping", localPath)
			return nil
		} else if err != nil && !os.IsNotExist(err) {
			log.Println("Failed to stat file", localPath, err)
			return err
		}
	}

	log.Println("   Start to https download:", localPath)
	multipart_download_finished := false
	thisdir := filepath.Dir(localPath)
	if err := os.MkdirAll(thisdir, 0755); err != nil {
		log.Println("Failed to create directories:", localPath, err)
		return err
	}
	file, err := os.OpenFile(localPath+".obstmp", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Failed to create obstmp file:", localPath, err)
		return err
	}
	defer func() {
		file.Close()
		if multipart_download_finished {
			if _, err := os.Stat(localPath + ".obstmp"); err == nil {
				if err := os.Rename(localPath+".obstmp", localPath); err != nil {
					log.Println(err, localPath)
				}
			} else if !os.IsNotExist(err) {
				log.Println(err, localPath)
			}
		}
	}()

	fileInfo := FileInfo{
		FromKey:    fullPrefix,
		FromBucket: from.bucket,
		Size:       fileSize,
		File:       file,
	}
	indexList, chunkSizeAuto := split(fileInfo, cfg.ChunkSize)
	partnumberList, _ := getDownloadedParts(fileInfo)
	if len(partnumberList) != 0 {
		log.Printf("Exist %d/%d parts on local path: %s, %v\n", len(partnumberList), len(indexList), localPath+".obstmp", partnumberList)
	}

	var wg2 sync.WaitGroup
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers * 4))
	for i, offset := range indexList {
		if !contains(partnumberList, i+1) {
			size := chunkSizeAuto
			if offset+chunkSizeAuto > fileInfo.Size {
				size = fileInfo.Size - offset
			}
			partInfo := PartInfo{
				FromBucket: from.bucket,
				FromKey:    fullPrefix,
				URL:        thisUrl,
				PartNumber: int64(i + 1),
				Size:       size,
				Offset:     offset,
				TotalParts: int64(len(indexList)),
			}
			semPart.Acquire(context.Background(), 1)
			atomic.AddInt32(&runningGoroutines, 1)
			wg2.Add(1)
			go downloadHttpChunk(partInfo, fileInfo.File, &wg2, semPart)
		}
	}
	wg2.Wait()
	deleteDownloadParts(fileInfo)
	multipart_download_finished = true
	log.Println("   Finish https download:", localPath)
	atomic.AddInt64(&objectCount, 1)
	atomic.AddInt64(&sizeCount, fileSize)
	return nil
}

func downloadHttpChunk(partInfo PartInfo, file *os.File, wg *sync.WaitGroup, semPart *semaphore.Weighted) error {
	defer wg.Done()
	defer semPart.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)
	buffer, err := getHTTPFileBody(partInfo)
	if err != nil {
		return err
	}
	if _, err := file.WriteAt(buffer, partInfo.Offset); err != nil {
		log.Printf("Failed to write part obs://%s part:%d/%d, err: %v\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts, err)
		return err
	}
	recordDownloadPart(partInfo)
	log.Printf("===Downloaded part obs://%s part:%d/%d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts)
	return nil
}

func getHTTPFileBody(partInfo PartInfo) ([]byte, error) {
	log.Printf("-->Downloading part obs://%s %d/%d, runningGoroutines: %d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
	req, err := http.NewRequest("GET", partInfo.URL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partInfo.Offset, partInfo.Offset+partInfo.Size-1))
	client := &http.Client{Transport: &RetryRoundTripper{Proxied: http.DefaultTransport, Retries: 3, Delay: time.Second * 5}}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error downloading chunk:", err)
		return nil, err
	}
	defer resp.Body.Close()
	buffer := make([]byte, partInfo.Size)
	_, err = io.ReadFull(resp.Body, buffer)
	if err != nil {
		fmt.Println("Error reading chunk:", err)
		return nil, err
	}
	return buffer, nil
}

func getHTTPFileSize(thisUrl string) (int64, error) {
	req, err := http.NewRequest("GET", thisUrl, nil)
	if err != nil {
		log.Println("Failed to create request for:", thisUrl, err)
		return 0, err
	}
	req.Header.Set("Range", "bytes=0-0")
	client := &http.Client{Transport: &RetryRoundTripper{Proxied: http.DefaultTransport, Retries: 3, Delay: time.Second * 5}}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Failed to GET file size for:", thisUrl, err)
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		log.Println("unexpected status code while GET file size:", resp.StatusCode, thisUrl)
		return 0, err
	}
	fileSizeStr := resp.Header.Get("Content-Range")
	if fileSizeStr == "" {
		log.Println("missing Content-Range header while GET file size for:", thisUrl)
		return 0, err
	}
	parts := strings.Split(fileSizeStr, "/")
	if len(parts) != 2 {
		log.Println("invalid Content-Range header format while GET file size for:", thisUrl)
		return 0, err
	}
	fileSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		log.Println("Failed to parse file size for:", thisUrl, err)
		return 0, err
	}
	return fileSize, nil
}
