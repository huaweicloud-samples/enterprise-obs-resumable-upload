package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"log"
	"mime"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/sync/semaphore"
)

func obsToObs(from, to BInfo) error {
	ignoreList := getIgnoreList()
	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers))

	targetObjectList := make([]obs.Content, 0)
	var err error
	if cfg.ListTarget && !cfg.SkipCompare {
		targetObjectList, err = getObjectList(to)
		if err != nil {
			return err
		}
	}
	multipartUploadsList, _ := getMultipartUploadList(to.client, to.bucket, to.prefix)

	objectList, err := listObjectsFlat(from.client, from.bucket, from.prefix)
	if err != nil {
		log.Println("Failed to list objects", err)
		return err
	}
	for _, item := range objectList {
		if strings.HasSuffix(item.Key, "/") {
			log.Println("...Skipping directory", item.Key)
			continue
		}
		if isIgnored(item.Key, ignoreList) {
			log.Println("...Skiping ignored key in ignoreList", item.Key)
		}
		var combinedKey string
		if item.Key != from.prefix {
			combinedKey = path.Join(to.prefix, strings.TrimPrefix(item.Key, from.prefix))
			combinedKey = strings.TrimPrefix(combinedKey, "/")
		} else {
			combinedKey = path.Join(to.prefix, path.Base(item.Key))
		}
		contentType := mime.TypeByExtension(filepath.Ext(item.Key))
		fileInfo := FileInfo{
			FromBucket: from.bucket,
			FromKey:    item.Key,
			ToBucket:   to.bucket,
			ToKey:      combinedKey,
			Size:       item.Size,
			Others:     MetaStruct{ContentType: contentType},
		}
		err = obsToObsAction(from, to, fileInfo, semFile, &wg, multipartUploadsList, targetObjectList)
		if err != nil {
			log.Println("Failed to obsToObsAction", err)
			return err
		}
	}
	wg.Wait()
	return err
}

func obsToObsAction(from, to BInfo, fileInfo FileInfo, semFile *semaphore.Weighted, wg *sync.WaitGroup, multipartUploadsList []obs.Upload, targetObjectList []obs.Content) error {
	if cfg.TransferMetadata {
		err := getMetadata(from, &fileInfo)
		if err != nil {
			return err
		}
	}
	uploadId, err := getUploadId(to.client, fileInfo, multipartUploadsList, targetObjectList)
	if err != nil {
		return err
	}
	if uploadId == "NEXT" {
		log.Printf("...File exists and same size. Skipping target. obs://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
		return nil
	}
	semFile.Acquire(context.Background(), 1)
	atomic.AddInt32(&runningGoroutines, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer semFile.Release(1)
		defer atomic.AddInt32(&runningGoroutines, -1)
		if fileInfo.Size < cfg.ResumableThreshold {
			err := transferSmall(from, to, fileInfo)
			if err != nil {
				log.Println("Failed to transferSmall", err)
				return
			}
		} else {
			log.Printf("   Start to transfer (>= ResumableThreshold) obs://%s, runningGoroutines: %d\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), runningGoroutines)
			err := transferMultipart(from, to, uploadId, fileInfo)
			if err != nil {
				log.Println("Failed to multipartProccess", err)
				return
			}
		}
		log.Printf("***Successfully transfered obs://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
		atomic.AddInt64(&objectCount, 1)
		atomic.AddInt64(&sizeCount, fileInfo.Size)
	}()
	return nil
}

func transferSmall(from, to BInfo, fileInfo FileInfo) error {
	log.Printf("   Start to download (< ResumableThreshold) from %s/%s, runningGoroutines: %d\n", fileInfo.FromBucket, fileInfo.FromKey, runningGoroutines)
	inputDownload := &obs.GetObjectInput{}
	inputDownload.Bucket = from.bucket
	inputDownload.Key = fileInfo.FromKey
	resp, err := from.client.GetObject(inputDownload)
	if err != nil {
		log.Println("Error download from", from.url, err)
		return err
	}
	buff, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Println("Error reading body from", from.url, err)
		return err
	}
	md5Hash := md5.Sum(buff)
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])
	log.Printf("   Start to upload (< ResumableThreshold) to %s/%s, runningGoroutines: %d\n", fileInfo.ToBucket, fileInfo.ToKey, runningGoroutines)
	inputUpload := &obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: fileInfo.ToBucket,
				Key:    fileInfo.ToKey,
			},
			ContentMD5: md5Str,
		},
		Body: bytes.NewReader(buff),
	}
	if to.storageClass != "" {
		inputUpload.StorageClass = obs.StorageClassType(to.storageClass)
	}
	if to.ACL != "" {
		inputUpload.ACL = obs.AclType(to.ACL)
	}
	if fileInfo.Others.ContentType != "" {
		inputUpload.ContentType = fileInfo.Others.ContentType
	}
	if cfg.TransferMetadata {
		inputUpload.Metadata = fileInfo.Others.Metadata
		inputUpload.ContentEncoding = fileInfo.Others.ContentEncoding
		inputUpload.ContentLanguage = fileInfo.Others.ContentLanguage
		inputUpload.CacheControl = fileInfo.Others.CacheControl
		inputUpload.ContentDisposition = fileInfo.Others.ContentDisposition
	}
	_, err = to.client.PutObject(inputUpload)
	if err != nil {
		log.Println("Error upload to", to.url, err)
		return err
	}
	return nil
}

func transferPart(from, to BInfo, partInfo PartInfo, wg *sync.WaitGroup, sem *semaphore.Weighted, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex) error {
	defer wg.Done()
	defer sem.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)
	buffer, err := downloadPartAction(from.client, partInfo)
	if err != nil {
		return err
	}
	err = uploadPartAction(buffer, partInfo, to.client, uploadId, partnumberList, partnumberListMutex)
	if err != nil {
		return err
	}
	return nil
}
