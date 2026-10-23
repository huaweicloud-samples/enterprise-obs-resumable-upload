package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"golang.org/x/sync/semaphore"
)

// JobRecord describes a single object change to transfer.
type JobRecord struct {
	Bucket struct {
		Name string
	}
	Object struct {
		Key  string
		Size int64
	}
}

// JobMessage is the payload produced to / consumed from the DMS Kafka topic.
type JobMessage struct {
	Records   []JobRecord
	Event     string
	EventName string
}

// DMSClient wraps a DMS Kafka instance used for distributed transfer orchestration.
type DMSClient struct {
	brokers  []string
	topic    string
	group    string
	user     string
	password string
}

func getDMSClient() (*DMSClient, error) {
	brokers := strings.Split(cfg.DMSBrokers, ",")
	clean := make([]string, 0, len(brokers))
	for _, b := range brokers {
		b = strings.TrimSpace(b)
		if b != "" {
			clean = append(clean, b)
		}
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("dms-brokers is required for DMS work modes")
	}
	return &DMSClient{
		brokers:  clean,
		topic:    cfg.DMSTopic,
		group:    cfg.DMSGroup,
		user:     cfg.DMSUser,
		password: cfg.DMSPassword,
	}, nil
}

func (c *DMSClient) producerConfig() *sarama.Config {
	sc := sarama.NewConfig()
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Net.DialTimeout = time.Duration(cfg.HttpTimeout) * time.Second
	sc.Net.WriteTimeout = time.Duration(cfg.HttpTimeout) * time.Second
	sc.Net.ReadTimeout = time.Duration(cfg.HttpTimeout) * time.Second
	if c.user != "" {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = c.user
		sc.Net.SASL.Password = c.password
		sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	return sc
}

func (c *DMSClient) consumeConfig() *sarama.Config {
	sc := sarama.NewConfig()
	sc.Consumer.Return.Errors = true
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	sc.Net.DialTimeout = time.Duration(cfg.HttpTimeout) * time.Second
	if c.user != "" {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = c.user
		sc.Net.SASL.Password = c.password
		sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	return sc
}

// compareBucket lists source and target, computes the delta, and either produces
// the delta to the DMS topic (producer != nil) or just writes the job list locally.
func compareBucket(from, to BInfo, dmsClient *DMSClient) error {
	var wg sync.WaitGroup
	var ignoreList []string
	var fromList, toList []obs.Content
	var jobList []JobMessage

	wg.Add(3)
	go func() {
		defer wg.Done()
		ignoreList = getIgnoreList()
	}()
	go func() {
		defer wg.Done()
		var e error
		fromList, e = getObjectList(from)
		if e != nil {
			log.Fatalln(e)
		}
	}()
	go func() {
		defer wg.Done()
		var e error
		toList, e = getObjectList(to)
		if e != nil {
			log.Fatalln(e)
		}
	}()
	wg.Wait()

	jobList, sizeCount = compareObjects(fromList, toList, ignoreList, from, to)
	objectCount = int64(len(jobList))

	if dmsClient != nil {
		log.Printf("Found %d jobs to send to DMS topic: %s\n", objectCount, dmsClient.topic)
		if err := dmsClient.produce(jobList); err != nil {
			log.Println("Failed to produce DMS messages", err)
		}
	}

	if cfg.JobListPath != "" {
		if err := writeJobListFile(jobList, cfg.JobListPath); err != nil {
			log.Println("Failed to write job list file", err)
		}
	}
	return nil
}

func compareObjects(fromList, toList []obs.Content, ignoreList []string, from, to BInfo) ([]JobMessage, int64) {
	var listSizeCount int64
	delta := make([]JobMessage, 0)
	fromMap := make(map[string]obs.Content)
	toMap := make(map[string]obs.Content)
	for _, obj := range fromList {
		if !isIgnored(obj.Key, ignoreList) {
			fromMap[obj.Key] = obj
		}
	}
	for _, obj := range toList {
		toMap[obj.Key] = obj
	}
	for key, fromObj := range fromMap {
		toKey := path.Join(to.prefix, strings.TrimPrefix(key, from.prefix))
		toObj, ok := toMap[toKey]
		if !ok || toObj.Size != fromObj.Size {
			record := JobRecord{}
			record.Bucket.Name = from.bucket
			record.Object.Key = key
			record.Object.Size = fromObj.Size
			msg := JobMessage{Records: []JobRecord{record}, EventName: "ObjectCreated:Put"}
			delta = append(delta, msg)
			listSizeCount += fromObj.Size
		}
	}
	return delta, listSizeCount
}

func (c *DMSClient) produce(jobList []JobMessage) error {
	producer, err := sarama.NewSyncProducer(c.brokers, c.producerConfig())
	if err != nil {
		return err
	}
	defer producer.Close()
	var logFile *os.File
	if cfg.DMSSentLogName != "" {
		dateTimePrefix := time.Now().Format("20060102150405")
		logPath := fmt.Sprintf("%s-%s-dms-sent.log", cfg.DMSSentLogName, dateTimePrefix)
		logFile, err = os.Create(logPath)
		if err != nil {
			log.Printf("Failed to create DMS sent log: %v\n", err)
		} else {
			defer logFile.Close()
		}
	}
	for i, job := range jobList {
		jobJSON, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %v", err)
		}
		msg := &sarama.ProducerMessage{
			Topic: c.topic,
			Key:   sarama.StringEncoder(fmt.Sprint(i)),
			Value: sarama.ByteEncoder(jobJSON),
		}
		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send DMS message: %v\n", err)
			continue
		}
		if logFile != nil {
			logFile.WriteString(string(jobJSON) + "\n")
		}
	}
	log.Printf("Complete produce job to topic: %s\n", c.topic)
	return nil
}

func consumeDMS(c *DMSClient) error {
	consumer, err := sarama.NewConsumerGroup(c.brokers, c.group, c.consumeConfig())
	if err != nil {
		return err
	}
	defer consumer.Close()
	handler := &dmsConsumerHandler{client: c}
	ctx := context.Background()
	for {
		if err := consumer.Consume(ctx, []string{c.topic}, handler); err != nil {
			log.Printf("DMS consume error: %v\n", err)
			time.Sleep(time.Duration(cfg.RetryDelay) * time.Second)
		}
	}
}

type dmsConsumerHandler struct {
	client *DMSClient
}

func (h *dmsConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dmsConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dmsConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers))
	ignoreList := getIgnoreList()
	for msg := range claim.Messages() {
		var jm JobMessage
		if err := json.Unmarshal(msg.Value, &jm); err != nil {
			log.Printf("Failed to parse DMS message: %v\n", err)
			session.MarkMessage(msg, "")
			continue
		}
		for _, record := range jm.Records {
			decodedKey, err := url.QueryUnescape(record.Object.Key)
			if err != nil {
				log.Printf("Failed to decode key: %v\n", err)
				continue
			}
			if isIgnored(decodedKey, ignoreList) {
				log.Printf("Skipping ignored key in ignoreList %s\n", decodedKey)
				continue
			}
			toKey := path.Join(to.prefix, strings.TrimPrefix(decodedKey, from.prefix))
			fileInfo := FileInfo{
				FromKey:    decodedKey,
				FromBucket: record.Bucket.Name,
				Size:       record.Object.Size,
				ToBucket:   to.bucket,
				ToKey:      toKey,
			}
			if strings.HasPrefix(jm.EventName, "ObjectCreated:") {
				targetObjectList := make([]obs.Content, 0)
				multipartUploadsList := make([]obs.Upload, 0)
				if fileInfo.Size >= cfg.ResumableThreshold {
					multipartUploadsList, _ = getMultipartUploadList(to.client, fileInfo.ToBucket, fileInfo.ToKey)
				}
				semFile.Acquire(context.Background(), 1)
				err := obsToObsAction(from, to, fileInfo, semFile, &sync.WaitGroup{}, multipartUploadsList, targetObjectList)
				semFile.Release(1)
				if err != nil {
					log.Printf("Failed to transfer object: %v\n", err)
					continue
				}
			}
			if strings.HasPrefix(jm.EventName, "ObjectRemoved:") {
				if err := delObject(to.client, fileInfo.ToBucket, fileInfo.ToKey); err != nil {
					log.Printf("Failed to delete object: %v\n", err)
				}
			}
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func writeJobListFile(jobList []JobMessage, p string) error {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		file, err := os.Create(p)
		if err != nil {
			fmt.Println("Error creating file: ", err)
			return err
		}
		file.Close()
	}
	file, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file: ", err)
		return err
	}
	defer file.Close()
	for _, job := range jobList {
		jobJSON, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %v", err)
		}
		_, err = file.WriteString(string(jobJSON) + "\n")
		if err != nil {
			return err
		}
	}
	log.Println("Complete writing job list to file", p)
	return nil
}
