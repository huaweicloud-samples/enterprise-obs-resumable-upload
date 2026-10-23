package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/sync/semaphore"
)

func checkPartnumberList(client *obs.ObsClient, bucket, key, uploadId string) ([]PartInfo, error) {
	var partNumbers []PartInfo
	var partNumbersPrint []int64
	input := &obs.ListPartsInput{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadId,
	}
	for {
		out, err := client.ListParts(input)
		if err != nil {
			return nil, err
		}
		for _, part := range out.Parts {
			partNumbers = append(partNumbers, PartInfo{
				PartNumber: int64(part.PartNumber),
				Etag:       part.ETag,
			})
			partNumbersPrint = append(partNumbersPrint, int64(part.PartNumber))
		}
		if !out.IsTruncated {
			break
		}
		input.PartNumberMarker = out.NextPartNumberMarker
	}
	log.Printf("   Exist %d parts on obs://%s :%v\n", len(partNumbersPrint), path.Join(bucket, key), partNumbersPrint)
	return partNumbers, nil
}

func checkFileExistHead(client *obs.ObsClient, tofileInfo FileInfo, multipartUploadsList []obs.Upload) (string, error) {
	exist, err := headFile(client, tofileInfo)
	if err != nil {
		return "", err
	}
	if exist {
		return "NEXT", nil
	}
	uploadId, err := checkMultipartUploadId(tofileInfo, multipartUploadsList)
	return uploadId, err
}

func compareMetaStructs(meta1, meta2 MetaStruct) bool {
	if meta1.ContentType != meta2.ContentType ||
		meta1.ContentLanguage != meta2.ContentLanguage ||
		meta1.ContentEncoding != meta2.ContentEncoding ||
		meta1.CacheControl != meta2.CacheControl ||
		meta1.ContentDisposition != meta2.ContentDisposition {
		return false
	}
	if len(meta1.Metadata) != len(meta2.Metadata) {
		return false
	}
	for k, v := range meta1.Metadata {
		if v2, ok := meta2.Metadata[k]; !ok || v != v2 {
			return false
		}
	}
	return true
}

func headFile(client *obs.ObsClient, tofileInfo FileInfo) (bool, error) {
	log.Printf("   Call HEAD to compare target obs://%s\n", path.Join(tofileInfo.ToBucket, tofileInfo.ToKey))
	input := &obs.GetObjectMetadataInput{
		Bucket: tofileInfo.ToBucket,
		Key:    tofileInfo.ToKey,
	}
	result, err := client.GetObjectMetadata(input)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "404") || strings.Contains(msg, "NoSuchKey") ||
			strings.Contains(msg, "NotFound") || strings.Contains(msg, "NoSuchBucket") {
			return false, nil
		}
		return false, err
	}
	if result.ContentLength == tofileInfo.Size {
		if cfg.TransferMetadata {
			log.Printf("   Comparing metadata of target obs://%s\n", path.Join(tofileInfo.ToBucket, tofileInfo.ToKey))
			resultStruct := MetaStruct{
				Metadata:           result.Metadata,
				ContentType:        result.ContentType,
				ContentLanguage:    result.ContentLanguage,
				ContentEncoding:    result.ContentEncoding,
				CacheControl:       result.CacheControl,
				ContentDisposition: result.ContentDisposition,
			}
			if !compareMetaStructs(resultStruct, tofileInfo.Others) {
				return false, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func checkFileExistList(tofileInfo FileInfo, targetObjectList []obs.Content, multipartUploadsList []obs.Upload) (string, error) {
	for _, f := range targetObjectList {
		if f.Key == tofileInfo.ToKey && f.Size == tofileInfo.Size {
			return "NEXT", nil
		}
	}
	uploadId, err := checkMultipartUploadId(tofileInfo, multipartUploadsList)
	return uploadId, err
}

func checkMultipartUploadId(tofileInfo FileInfo, multipartUploadsList []obs.Upload) (string, error) {
	if tofileInfo.Size < cfg.ResumableThreshold {
		return "", nil
	}
	var keyIDList []obs.Upload
	for _, u := range multipartUploadsList {
		if u.Key == tofileInfo.ToKey {
			keyIDList = append(keyIDList, u)
		}
	}
	if len(keyIDList) == 0 {
		return "", nil
	}
	var latestUpload *obs.Upload
	for i := range keyIDList {
		u := &keyIDList[i]
		if latestUpload == nil || u.Initiated.After(latestUpload.Initiated) {
			latestUpload = u
		}
	}
	return latestUpload.UploadId, nil
}

func getUploadId(client *obs.ObsClient, fileInfo FileInfo, multipartUploadsList []obs.Upload, targetObjectList []obs.Content) (string, error) {
	var uploadId string
	var err error
	if !cfg.SkipCompare {
		if cfg.TransferMetadata || !cfg.ListTarget {
			uploadId, err = checkFileExistHead(client, fileInfo, multipartUploadsList)
			if err != nil {
				log.Printf("failed to checkFileExistHead, %v", err)
				return "", err
			}
		} else if cfg.ListTarget && !cfg.TransferMetadata {
			uploadId, err = checkFileExistList(fileInfo, targetObjectList, multipartUploadsList)
			if err != nil {
				log.Printf("failed to checkFileExistList, %v", err)
				return "", err
			}
		}
	}
	return uploadId, nil
}

func getMultipartUploadList(client *obs.ObsClient, bucket string, prefix string) ([]obs.Upload, error) {
	var uploads []obs.Upload
	input := &obs.ListMultipartUploadsInput{
		Bucket: bucket,
		Prefix: prefix,
	}
	for {
		out, err := client.ListMultipartUploads(input)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, out.Uploads...)
		if !out.IsTruncated {
			break
		}
		input.KeyMarker = out.NextKeyMarker
		input.UploadIdMarker = out.NextUploadIdMarker
	}
	log.Printf("There are %d multipart uploads ID already in target obs://%s\n", len(uploads), path.Join(bucket, prefix))
	return uploads, nil
}

func getObjectList(b BInfo) ([]obs.Content, error) {
	log.Printf("Listing obs://%s\n", path.Join(b.bucket, b.prefix))
	var contents []obs.Content
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(cfg.NumWorkers * 4))
	concurrencyListObjects(b.client, b.bucket, b.prefix, sem, &contents, &mu, &wg)
	wg.Wait()
	log.Printf("There are %d objects already in obs://%s\n", len(contents), path.Join(b.bucket, b.prefix))
	return contents, nil
}

func listObjectsFlat(client *obs.ObsClient, bucket, prefix string) ([]obs.Content, error) {
	var contents []obs.Content
	input := &obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{Prefix: prefix},
		Bucket:        bucket,
	}
	for {
		out, err := client.ListObjects(input)
		if err != nil {
			return nil, err
		}
		contents = append(contents, out.Contents...)
		if !out.IsTruncated {
			break
		}
		input.Marker = out.NextMarker
		if input.Marker == "" && len(out.Contents) > 0 {
			input.Marker = out.Contents[len(out.Contents)-1].Key
		}
	}
	return contents, nil
}

func concurrencyListObjects(client *obs.ObsClient, bucket, prefix string, sem *semaphore.Weighted, contents *[]obs.Content, mu *sync.Mutex, wg *sync.WaitGroup) {
	input := &obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{Delimiter: "/", Prefix: prefix},
		Bucket:        bucket,
	}
	for {
		out, err := client.ListObjects(input)
		if err != nil {
			fmt.Printf("Error listing objects: %v", err)
			return
		}
		mu.Lock()
		*contents = append(*contents, out.Contents...)
		mu.Unlock()
		for _, cp := range out.CommonPrefixes {
			wg.Add(1)
			go func(p string) {
				defer sem.Release(1)
				defer wg.Done()
				sem.Acquire(context.Background(), 1)
				concurrencyListObjects(client, bucket, p, sem, contents, mu, wg)
			}(cp)
		}
		if !out.IsTruncated {
			break
		}
		input.Marker = out.NextMarker
		if input.Marker == "" && len(out.Contents) > 0 {
			input.Marker = out.Contents[len(out.Contents)-1].Key
		}
	}
}

func getMetadata(b BInfo, fileInfo *FileInfo) error {
	log.Printf("-->Get metadata obs://%s\n", path.Join(fileInfo.FromBucket, fileInfo.FromKey))
	headResp, err := b.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: fileInfo.FromBucket,
		Key:    fileInfo.FromKey,
	})
	if err != nil {
		log.Printf("failed to get object metadata, %v", err)
		return err
	}
	fileInfo.Others = MetaStruct{
		Metadata:           headResp.Metadata,
		ContentType:        headResp.ContentType,
		ContentLanguage:    headResp.ContentLanguage,
		ContentEncoding:    headResp.ContentEncoding,
		CacheControl:       headResp.CacheControl,
		ContentDisposition: headResp.ContentDisposition,
	}
	return nil
}

func delObject(client *obs.ObsClient, bucket, key string) error {
	_, err := client.DeleteObject(&obs.DeleteObjectInput{Bucket: bucket, Key: key})
	return err
}
