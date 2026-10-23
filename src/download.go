package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/sync/semaphore"
)

func startDownload(from, to BInfo) error {
	var wg sync.WaitGroup
	var err error
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers))
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers * 4))
	ignoreList := getIgnoreList()

	objectList, err := listObjectsFlat(from.client, from.bucket, from.prefix)
	if err != nil {
		log.Println("Failed to list objects", err)
		return err
	}
	for _, item := range objectList {
		if strings.HasSuffix(item.Key, "/") {
			log.Println("...Skiping directory", item.Key)
			continue
		}
		if isIgnored(item.Key, ignoreList) {
			log.Println("...Skiping ignored key in ignoreList", item.Key)
		}
		var combinedKey string
		if item.Key != from.prefix {
			combinedKey = strings.TrimPrefix(item.Key, from.prefix)
		} else {
			combinedKey = filepath.Base(item.Key)
		}
		localPath := filepath.Join(to.url, combinedKey)
		info, err := os.Stat(localPath)
		if !cfg.SkipCompare {
			if err == nil && info.Size() == item.Size {
				log.Println("...File exists and same size, skipping", localPath)
				continue
			} else if err != nil && !os.IsNotExist(err) {
				log.Println("Failed to stat file", localPath, err)
				continue
			}
		}
		thisdir := filepath.Dir(localPath)
		if err := os.MkdirAll(thisdir, 0755); err != nil {
			log.Println("Failed to create directories:", localPath, err)
			continue
		}
		semFile.Acquire(context.Background(), 1)
		atomic.AddInt32(&runningGoroutines, 1)
		wg.Add(1)
		go func(item obs.Content) {
			defer wg.Done()
			defer semFile.Release(1)
			defer atomic.AddInt32(&runningGoroutines, -1)
			if item.Size < cfg.ResumableThreshold {
				log.Println("   Start to download (<ResumableThreshold):", localPath, "runningGoroutines:", runningGoroutines)
				file, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Println("Failed to create file", localPath, err)
					return
				}
				defer file.Close()
				input := &obs.GetObjectInput{}
				input.Bucket = from.bucket
				input.Key = item.Key
				resp, err := from.client.GetObject(input)
				if err != nil {
					log.Println("Failed to download file", localPath, err)
					return
				}
				_, err = io.Copy(file, resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Println("Failed to write file", localPath, err)
					return
				}
			} else {
				log.Println("   Start to download (>=ResumableThreshold):", localPath, "runningGoroutines:", runningGoroutines)
				multipart_download_finished := false
				file, err := os.OpenFile(localPath+".obstmp", os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Println("Failed to create obstmp file", localPath, err)
					return
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
					FromKey:    item.Key,
					FromBucket: from.bucket,
					Size:       item.Size,
					File:       file,
				}
				indexList, chunkSizeAuto := split(fileInfo, cfg.ChunkSize)
				partnumberList, _ := getDownloadedParts(fileInfo)
				if len(partnumberList) != 0 {
					log.Printf("Exist %d/%d parts on local path: %s, %v\n", len(partnumberList), len(indexList), localPath+".obstmp", partnumberList)
				}
				var wg2 sync.WaitGroup
				for i, offset := range indexList {
					if !contains(partnumberList, i+1) {
						size := chunkSizeAuto
						if offset+chunkSizeAuto > fileInfo.Size {
							size = fileInfo.Size - offset
						}
						partInfo := PartInfo{
							FromKey:    fileInfo.FromKey,
							FromBucket: fileInfo.FromBucket,
							PartNumber: int64(i + 1),
							Size:       size,
							Offset:     offset,
							TotalParts: int64(len(indexList)),
						}
						semPart.Acquire(context.Background(), 1)
						atomic.AddInt32(&runningGoroutines, 1)
						wg2.Add(1)
						go downloadPart(from.client, partInfo, fileInfo.File, &wg2, semPart)
					}
				}
				wg2.Wait()
				deleteDownloadParts(fileInfo)
				multipart_download_finished = true
			}
			log.Println("***Successfully downloaded:", localPath)
			atomic.AddInt64(&objectCount, 1)
			atomic.AddInt64(&sizeCount, item.Size)
		}(item)
	}
	wg.Wait()
	return err
}

func downloadPartAction(client *obs.ObsClient, partInfo PartInfo) ([]byte, error) {
	log.Printf("-->Downloading part obs://%s %d/%d, runningGoroutines: %d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
	input := &obs.GetObjectInput{}
	input.Bucket = partInfo.FromBucket
	input.Key = partInfo.FromKey
	input.Range = fmt.Sprintf("bytes=%d-%d", partInfo.Offset, partInfo.Offset+partInfo.Size-1)
	resp, err := client.GetObject(input)
	if err != nil {
		log.Println("Failed to download part", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return nil, err
	}
	defer resp.Body.Close()
	buffer, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Failed to read from response body:", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return nil, err
	}
	return buffer, nil
}

func downloadPart(client *obs.ObsClient, partInfo PartInfo, file *os.File, wg *sync.WaitGroup, semPart *semaphore.Weighted) error {
	defer wg.Done()
	defer semPart.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)
	buffer, err := downloadPartAction(client, partInfo)
	if err != nil {
		return err
	}
	if _, err := file.WriteAt(buffer, partInfo.Offset); err != nil {
		log.Println("Failed to write to file", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return err
	}
	recordDownloadPart(partInfo)
	log.Printf("===Downloaded part obs://%s part:%d/%d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts)
	return nil
}
