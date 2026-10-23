package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/sync/semaphore"
)

func startUpload(from, to BInfo) error {
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
	multipartUploadsList, err := getMultipartUploadList(to.client, to.bucket, to.prefix)
	if err != nil {
		return err
	}
	err = filepath.Walk(from.url, func(thispath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Failed to access path", thispath, err)
			return err
		}
		if isIgnored(thispath, ignoreList) {
			log.Println("...Skiping ignored key in ignoreList", thispath)
		}
		if info.IsDir() {
			return nil
		}
		combinedKey := path.Join(to.prefix, filepath.ToSlash(strings.TrimPrefix(thispath, filepath.Dir(from.url))))
		contentType := mime.TypeByExtension(filepath.Ext(thispath))
		fileInfo := FileInfo{
			ToKey:    combinedKey,
			ToBucket: to.bucket,
			Size:     info.Size(),
			Others:   MetaStruct{ContentType: contentType},
		}
		uploadId, err := getUploadId(to.client, fileInfo, multipartUploadsList, targetObjectList)
		if err != nil {
			return err
		}
		if uploadId == "NEXT" {
			log.Printf("...File already exists. Skipping... obs://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
			return nil
		}
		semFile.Acquire(context.Background(), 1)
		atomic.AddInt32(&runningGoroutines, 1)
		wg.Add(1)
		go func(thispath string, info os.FileInfo, uploadId string) {
			defer wg.Done()
			defer semFile.Release(1)
			defer atomic.AddInt32(&runningGoroutines, -1)
			fileInfo.File, err = os.Open(thispath)
			if err != nil {
				log.Println("Failed to open file", thispath, err)
				return
			}
			defer fileInfo.File.Close()
			if info.Size() < cfg.ResumableThreshold {
				err := uploadSmall(fileInfo, thispath, info, uploadId)
				if err != nil {
					log.Printf("Failed to uploadSmall: obs://%s, %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
					return
				}
			} else {
				err := transferMultipart(from, to, uploadId, fileInfo)
				if err != nil {
					log.Printf("Failed to multipartProccess: obs://%s, %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
					return
				}
			}
			log.Printf("***Successfully uploaded: obs://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
			atomic.AddInt64(&objectCount, 1)
			atomic.AddInt64(&sizeCount, info.Size())
		}(thispath, info, uploadId)
		return nil
	})
	wg.Wait()
	if err != nil {
		log.Println("Failed to walk directory", err)
		return err
	}
	return err
}

func uploadSmall(fileInfo FileInfo, thispath string, info os.FileInfo, uploadId string) error {
	buff, err := io.ReadAll(fileInfo.File)
	if err != nil {
		log.Println("Failed to read file", thispath, err)
		return err
	}
	md5Hash := md5.Sum(buff)
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])
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
	_, err = to.client.PutObject(inputUpload)
	if err != nil {
		log.Printf("Failed to upload file obs://%s, err: %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
		return err
	}
	return nil
}

func transferMultipart(from, to BInfo, uploadId string, fileInfo FileInfo) error {
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers * 4))
	var partnumberList []PartInfo
	var partnumberListMutex sync.Mutex
	var fileMutex sync.Mutex
	var err error

	if uploadId == "" {
		inputCreate := &obs.InitiateMultipartUploadInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket: fileInfo.ToBucket,
				Key:    fileInfo.ToKey,
			},
		}
		if to.storageClass != "" {
			inputCreate.StorageClass = obs.StorageClassType(to.storageClass)
		}
		if to.ACL != "" {
			inputCreate.ACL = obs.AclType(to.ACL)
		}
		if fileInfo.Others.ContentType != "" {
			inputCreate.ContentType = fileInfo.Others.ContentType
		}
		if cfg.TransferMetadata {
			inputCreate.Metadata = fileInfo.Others.Metadata
			inputCreate.ContentEncoding = fileInfo.Others.ContentEncoding
			inputCreate.CacheControl = fileInfo.Others.CacheControl
			inputCreate.ContentLanguage = fileInfo.Others.ContentLanguage
			inputCreate.ContentDisposition = fileInfo.Others.ContentDisposition
		}
		resp, err := to.client.InitiateMultipartUpload(inputCreate)
		if err != nil {
			log.Println("Failed to create multipart upload", fileInfo.ToBucket, fileInfo.ToKey, err)
			return err
		}
		uploadId = resp.UploadId
	} else {
		partnumberList, err = checkPartnumberList(to.client, fileInfo.ToBucket, fileInfo.ToKey, uploadId)
		if err != nil {
			log.Println("Failed to get part number list", fileInfo.ToBucket, fileInfo.ToKey, err)
			return err
		}
	}

	indexList, chunkSizeAuto := split(fileInfo, cfg.ChunkSize)
	var wg2 sync.WaitGroup
	for i, offset := range indexList {
		found := false
		for _, value := range partnumberList {
			if int64(i+1) == value.PartNumber {
				found = true
				break
			}
		}
		if found {
			continue
		}
		size := chunkSizeAuto
		if offset+chunkSizeAuto > fileInfo.Size {
			size = fileInfo.Size - offset
		}
		partInfo := PartInfo{
			ToKey:      fileInfo.ToKey,
			ToBucket:   fileInfo.ToBucket,
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
		if fileInfo.File != nil {
			go uploadPart(to.client, partInfo, &wg2, semPart, uploadId, &partnumberList, &partnumberListMutex, fileInfo.File, &fileMutex)
		} else {
			go transferPart(from, to, partInfo, &wg2, semPart, uploadId, &partnumberList, &partnumberListMutex)
		}
	}
	wg2.Wait()
	if len(indexList) == len(partnumberList) {
		err := completeUpload(to.client, uploadId, fileInfo.ToBucket, fileInfo.ToKey, &partnumberList)
		if err != nil {
			log.Println("Failed to complete upload", err, fileInfo.ToBucket, fileInfo.ToKey)
			return err
		}
	} else {
		log.Println("Failed to complete upload, len(indexList) != len(partnumberList)", fileInfo.ToBucket, fileInfo.ToKey, len(indexList), len(partnumberList))
		return errors.New("failed to complete upload, len(indexList) != len(partnumberList)")
	}
	return nil
}

func uploadPart(client *obs.ObsClient, partInfo PartInfo, wg *sync.WaitGroup, sem *semaphore.Weighted, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex, file *os.File, fileMutex *sync.Mutex) error {
	defer wg.Done()
	defer sem.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)
	fileMutex.Lock()
	file.Seek(partInfo.Offset, 0)
	buffer := make([]byte, partInfo.Size)
	_, err := io.ReadFull(file, buffer)
	fileMutex.Unlock()
	if err != nil {
		log.Println("Failed to read full buffer from file", partInfo.ToBucket, partInfo.ToKey, partInfo.PartNumber, err)
		return err
	}
	err = uploadPartAction(buffer, partInfo, client, uploadId, partnumberList, partnumberListMutex)
	if err != nil {
		return err
	}
	return nil
}

func uploadPartAction(buff []byte, partInfo PartInfo, client *obs.ObsClient, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex) error {
	log.Printf("-->Uploading part obs://%s, part:%d/%d, runningGoroutines: %d\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
	md5Hash := md5.Sum(buff)
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])
	result, err := client.UploadPart(&obs.UploadPartInput{
		Bucket:     partInfo.ToBucket,
		Key:        partInfo.ToKey,
		PartNumber: int(partInfo.PartNumber),
		UploadId:   uploadId,
		Body:       bytes.NewReader(buff),
		ContentMD5: md5Str,
	})
	if err != nil {
		log.Printf("Failed to upload part obs://%s, part:%d/%d, err: %v\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts, err)
		return err
	}
	partnumberListMutex.Lock()
	*partnumberList = append(*partnumberList, PartInfo{
		PartNumber: partInfo.PartNumber,
		Etag:       result.ETag,
	})
	partnumberListMutex.Unlock()
	log.Printf("===Uploaded part obs://%s, part:%d/%d\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts)
	return nil
}

func completeUpload(client *obs.ObsClient, uploadId, bucket, key string, partnumberList *[]PartInfo) error {
	completedParts := []obs.Part{}
	var i int64
	for i = 1; i <= int64(len(*partnumberList)); i++ {
		for _, partNumber := range *partnumberList {
			if i == partNumber.PartNumber {
				completedParts = append(completedParts, obs.Part{
					ETag:       partNumber.Etag,
					PartNumber: int(partNumber.PartNumber),
				})
				break
			}
		}
	}
	_, err := client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadId,
		Parts:    completedParts,
	})
	return err
}
