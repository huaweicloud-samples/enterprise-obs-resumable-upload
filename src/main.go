// 多线程并发断点续传上传/下载对象存储，支持华为云 OBS 等标准对象存储服务的多段上传/分段下载
// 使用 ./obstrans -h 获取更多帮助信息
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	ListTarget         bool   `mapstructure:"list-target"`
	SkipCompare        bool   `mapstructure:"skip-compare"`
	TransferMetadata   bool   `mapstructure:"transfer-metadata"`
	HttpTimeout        int    `mapstructure:"http-timeout"`
	MaxRetries         int    `mapstructure:"max-retries"`
	ResumableThreshold int64  `mapstructure:"resumable-threshold"`
	NumWorkers         int    `mapstructure:"num-workers"`
	WorkMode           string `mapstructure:"work-mode"`
	DMSBrokers         string `mapstructure:"dms-brokers"`
	DMSTopic           string `mapstructure:"dms-topic"`
	DMSGroup           string `mapstructure:"dms-group"`
	DMSUser            string `mapstructure:"dms-user"`
	DMSPassword        string `mapstructure:"dms-password"`
	YPtr               bool   `mapstructure:"y"`
	DBPath             string `mapstructure:"db-location"`
	ChunkSize          int64  `mapstructure:"chunk-size"`
	RetryDelay         int    `mapstructure:"retry-delay"`
	JobListPath        string `mapstructure:"joblist-write-to-filepath"`
	DMSSentLogName     string `mapstructure:"dms-log-to-filename"`
	IgnoreListPath     string `mapstructure:"ignore-list-path"`
}

type BInfo struct {
	url, bucket, prefix, endpoint, region, storageClass, ACL string
	client                                                    *obs.ObsClient
}

type MetaStruct struct {
	Metadata                                                                        map[string]string
	ContentType, ContentLanguage, ContentEncoding, CacheControl, ContentDisposition string
}

type FileInfo struct {
	FromKey, FromBucket, ToKey, ToBucket string
	Size                                 int64
	File                                 *os.File
	Others                               MetaStruct
}

type PartInfo struct {
	FromKey, FromBucket, ToKey, ToBucket, Etag string
	Size, Offset                               int64
	PartNumber, TotalParts                     int64
	URL                                        string
}

type RetryFunc func() error

var (
	from, to               BInfo
	objectCount, sizeCount int64
	runningGoroutines      int32
	cfg                    Config
	dmsClient              *DMSClient
)

var rootCmd = &cobra.Command{
	Use:   "obstrans FROM_URL TO_URL",
	Short: "obstrans transfers data from source to target",
	Long: `obstrans transfers data from source to target.
	./obstrans FROM_URL TO_URL [OPTIONS]
	FROM_URL: The url of data source, e.g. /home/user/data or obs://bucket/prefix
	TO_URL: The url of data transfer target, e.g. /home/user/data or obs://bucket/prefix
	For example:
	./obstrans obs://bucket/prefix obs://bucket/prefix
	./obstrans obs://bucket/prefix /home/user/data
	`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		from.url = args[0]
		to.url = args[1]
	},
}

func init() {
	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Print(cmd.Long)
		os.Exit(0)
	})
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().String("from-endpoint", "", "The endpoint of data source, e.g. https://obs.cn-north-4.myhuaweicloud.com . If OBS or local path, no need to specify this.")
	viper.BindPFlag("from-endpoint", rootCmd.PersistentFlags().Lookup("from-endpoint"))
	rootCmd.PersistentFlags().String("to-endpoint", "", "The endpoint of data transfer target, e.g. https://obs.cn-north-4.myhuaweicloud.com . If OBS or local path, no need to specify this.")
	viper.BindPFlag("to-endpoint", rootCmd.PersistentFlags().Lookup("to-endpoint"))
	rootCmd.PersistentFlags().String("from-region", "", "The region of data transfer source, e.g. cn-north-4.")
	viper.BindPFlag("from-region", rootCmd.PersistentFlags().Lookup("from-region"))
	rootCmd.PersistentFlags().String("to-region", "", "The region of data transfer target, e.g. cn-north-4.")
	viper.BindPFlag("to-region", rootCmd.PersistentFlags().Lookup("to-region"))
	rootCmd.PersistentFlags().String("storage-class", "", "The TARGET bucket storage class, e.g. STANDARD|WARM|COLD or others.")
	viper.BindPFlag("storage-class", rootCmd.PersistentFlags().Lookup("storage-class"))
	rootCmd.PersistentFlags().String("acl", "", "The TARGET bucket ACL, private means only the object owner can read&write, e.g. private|public-read|public-read-write|authenticated-read|bucket-owner-full-control")
	viper.BindPFlag("acl", rootCmd.PersistentFlags().Lookup("acl"))
	rootCmd.PersistentFlags().String("db-location", "./download-status.db", "local db to record download resumable status")
	viper.BindPFlag("db-location", rootCmd.PersistentFlags().Lookup("db-location"))
	rootCmd.PersistentFlags().BoolP("list-target", "l", false, "List the TARGET bucket, compare exist objects BEFORE transfer.")
	viper.BindPFlag("list-target", rootCmd.PersistentFlags().Lookup("list-target"))
	rootCmd.PersistentFlags().BoolP("skip-compare", "s", false, "If True, skip to compare the name and size between source and target object. Just overwrite all objects.")
	viper.BindPFlag("skip-compare", rootCmd.PersistentFlags().Lookup("skip-compare"))
	rootCmd.PersistentFlags().Bool("transfer-metadata", false, "If True, get metadata from source bucket and upload the metadata to target object.")
	viper.BindPFlag("transfer-metadata", rootCmd.PersistentFlags().Lookup("transfer-metadata"))
	rootCmd.PersistentFlags().Int("http-timeout", 30, "API request timeout (seconds)")
	viper.BindPFlag("http-timeout", rootCmd.PersistentFlags().Lookup("http-timeout"))
	rootCmd.PersistentFlags().Int("max-retries", 5, "API request max retries")
	viper.BindPFlag("max-retries", rootCmd.PersistentFlags().Lookup("max-retries"))
	rootCmd.PersistentFlags().Int("retry-delay", 5, "Delay before next retry in seconds")
	viper.BindPFlag("retry-delay", rootCmd.PersistentFlags().Lookup("retry-delay"))
	rootCmd.PersistentFlags().Int64("chunk-size", 5, "Multipart part size(MB)")
	viper.BindPFlag("chunk-size", rootCmd.PersistentFlags().Lookup("chunk-size"))
	rootCmd.PersistentFlags().Int64("resumable-threshold", 50, "When the file size (MB) is larger than this value, the file will be resumable transfered.")
	viper.BindPFlag("resumable-threshold", rootCmd.PersistentFlags().Lookup("resumable-threshold"))
	rootCmd.PersistentFlags().IntP("num-workers", "n", 4, "Max concurrent threads = NumWorkers*NumWorkers*4 (files*parts*4), recommend NumWorkers <= vCPU number")
	viper.BindPFlag("num-workers", rootCmd.PersistentFlags().Lookup("num-workers"))
	rootCmd.PersistentFlags().BoolP("y", "y", false, "Ignore waiting for confirming command")
	viper.BindPFlag("y", rootCmd.PersistentFlags().Lookup("y"))
	rootCmd.PersistentFlags().String("work-mode", "", "DMS_PRODUCE | DMS_CONSUME | DRYRUN | HTTP_DOWNLOAD_LIST; DMS_PRODUCE means listing source FROM_URL and target TO_URL to compare and send message to DMS topic, DMS_CONSUME means consume message from DMS topic and transfer objects; DRYRUN means only count the objects and sizes comparing delta list; HTTP_DOWNLOAD_LIST, from a list file with lines of presign url;")
	viper.BindPFlag("work-mode", rootCmd.PersistentFlags().Lookup("work-mode"))
	rootCmd.PersistentFlags().String("dms-brokers", "", "Comma-separated DMS Kafka broker addresses, e.g. 192.168.0.1:9092,192.168.0.2:9092")
	viper.BindPFlag("dms-brokers", rootCmd.PersistentFlags().Lookup("dms-brokers"))
	rootCmd.PersistentFlags().String("dms-topic", "", "The DMS Kafka topic to produce or consume messages")
	viper.BindPFlag("dms-topic", rootCmd.PersistentFlags().Lookup("dms-topic"))
	rootCmd.PersistentFlags().String("dms-group", "obstrans-group", "The DMS Kafka consumer group id")
	viper.BindPFlag("dms-group", rootCmd.PersistentFlags().Lookup("dms-group"))
	rootCmd.PersistentFlags().String("dms-user", "", "The DMS Kafka SASL username (optional)")
	viper.BindPFlag("dms-user", rootCmd.PersistentFlags().Lookup("dms-user"))
	rootCmd.PersistentFlags().String("dms-password", "", "The DMS Kafka SASL password (optional)")
	viper.BindPFlag("dms-password", rootCmd.PersistentFlags().Lookup("dms-password"))
	rootCmd.PersistentFlags().String("joblist-write-to-filepath", "", "After listing source and target, compare the delta joblist and write the joblist to this filepath, e.g. ./joblist.txt")
	viper.BindPFlag("joblist-write-to-filepath", rootCmd.PersistentFlags().Lookup("joblist-write-to-filepath"))
	rootCmd.PersistentFlags().String("dms-log-to-filename", "", "After sent joblist to DMS, write the sent messages log to this filepath, e.g. ./dms-log.txt")
	viper.BindPFlag("dms-log-to-filename", rootCmd.PersistentFlags().Lookup("dms-log-to-filename"))
	rootCmd.PersistentFlags().String("ignore-list-path", "", "When listing and transfer, if source key or local path matching the prefix in this ignore-list, it will be ignored.")
	viper.BindPFlag("ignore-list-path", rootCmd.PersistentFlags().Lookup("ignore-list-path"))
}

func initConfig() {
	viper.AutomaticEnv() // read in environment variables that match
	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
	if err := viper.Unmarshal(&cfg); err != nil {
		fmt.Println("Error unmarshalling config:", err)
		os.Exit(1)
	}
}

func getConfig() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	from.endpoint = viper.GetString("from-endpoint")
	to.endpoint = viper.GetString("to-endpoint")
	from.region = viper.GetString("from-region")
	to.region = viper.GetString("to-region")
	to.storageClass = viper.GetString("storage-class")
	to.ACL = viper.GetString("acl")
	cfg.ResumableThreshold = cfg.ResumableThreshold * 1024 * 1024
	cfg.ChunkSize = cfg.ChunkSize * 1024 * 1024

	for i, binfo := range []*BInfo{&from, &to} {
		if i == 0 {
			fmt.Print("From ")
		} else {
			fmt.Print("To ")
		}
		if strings.HasPrefix(binfo.url, "obs://") {
			u, err := url.Parse(binfo.url)
			if err != nil {
				log.Fatalf("Invalid OBS URL: %s, %v\n", binfo.url, err)
				os.Exit(1)
			}
			binfo.bucket = u.Host
			binfo.prefix = strings.TrimSuffix(strings.TrimPrefix(u.Path, "/"), "/")
			binfo.client = getClient(binfo)
			fmt.Printf("Bucket: %s, Prefix: %s, Endpoint: %s, Region:%s\n", binfo.bucket, binfo.prefix, binfo.endpoint, binfo.region)
		} else {
			if strings.HasPrefix(binfo.url, "http") {
				fmt.Printf("Presign URL: %s\n", binfo.url)
				continue
			}
			urlInfo, err := os.Stat(binfo.url)
			if err != nil {
				log.Printf("Invalid path, try to create directories: %s\n", binfo.url)
				if err := os.MkdirAll(binfo.url, 0755); err != nil {
					log.Fatalln("Failed to create directories:", binfo.url, err)
				}
			} else {
				if urlInfo.IsDir() && !strings.HasSuffix(binfo.url, string(os.PathSeparator)) {
					binfo.url += string(os.PathSeparator)
				}
				fmt.Printf("Local: %s\n", binfo.url)
			}
		}
	}
	if cfg.WorkMode == "DMS_PRODUCE" || cfg.WorkMode == "DMS_CONSUME" {
		var err error
		dmsClient, err = getDMSClient()
		if err != nil {
			log.Fatalln("Failed to init DMS client:", err)
		}
	}
}

func getClient(b *BInfo) *obs.ObsClient {
	ak := os.Getenv("HW_ACCESS_KEY")
	sk := os.Getenv("HW_SECRET_KEY")
	endpoint := b.endpoint
	if endpoint == "" && b.region != "" {
		endpoint = fmt.Sprintf("https://obs.%s.myhuaweicloud.com", b.region)
	}
	if endpoint == "" {
		log.Fatalln("No endpoint or region specified for obs:// URL")
	}
	client, err := obs.New(ak, sk, endpoint)
	if err != nil {
		log.Fatalf("Failed to create OBS client: %v\n", err)
	}
	return client
}

func main() {
	startTime := time.Now()
	getConfig()
	fmt.Printf(" Target StorageClass(default: STANDARD): %s\n Target ACL(default: private): %s\n", to.storageClass, to.ACL)
	fmt.Printf(" Transfer Metadata: %t\n List Target Before Transfer(Recommended): %t\n Skip Compare Before Transfer: %t\n", cfg.TransferMetadata, cfg.ListTarget, cfg.SkipCompare)
	fmt.Printf(" NumWorkers: %d for concurrency files; NumWorkers*4 for parts of each file and for listing target bucket\n", cfg.NumWorkers)
	fmt.Printf(" HttpTimeout: %ds\n MaxRetries: %d\n ResumableThreshold: %s\n", cfg.HttpTimeout, cfg.MaxRetries, ByteCountSI(cfg.ResumableThreshold))
	fmt.Printf(" ChunkSize: %s\n", ByteCountSI(cfg.ChunkSize))
	fmt.Printf(" WorkMode: %s\n", cfg.WorkMode)
	switch {
	case strings.ToUpper(cfg.WorkMode) == "DRYRUN":
		err := compareBucket(from, to, nil)
		if err != nil {
			log.Println("Failed to count:", err)
			return
		}
	case strings.ToUpper(cfg.WorkMode) == "DMS_PRODUCE":
		err := compareBucket(from, to, dmsClient)
		if err != nil {
			log.Println("Failed to produce DMS:", err)
			return
		}
	case strings.ToUpper(cfg.WorkMode) == "DMS_CONSUME":
		err := consumeDMS(dmsClient)
		if err != nil {
			log.Println("Failed to consume DMS:", err)
			return
		}
	case strings.HasPrefix(from.url, "obs://") && strings.HasPrefix(to.url, "obs://"):
		cfg.WorkMode = "OBSTOOBS"
		err := obsToObs(from, to)
		if err != nil {
			log.Println("Failed to transfer:", err)
			return
		}
	case strings.HasPrefix(from.url, "obs://"):
		cfg.WorkMode = "GET"
		err := startDownload(from, to)
		if err != nil {
			log.Println("Failed to download:", err)
			return
		}
	case strings.HasPrefix(to.url, "obs://"):
		cfg.WorkMode = "PUT"
		err := startUpload(from, to)
		if err != nil {
			log.Println("Failed to upload:", err)
			return
		}
	case strings.HasPrefix(from.url, "http"):
		cfg.WorkMode = "HTTP_DOWNLOAD"
		err := startHttpDownload(from, to)
		if err != nil {
			log.Println("Failed to download:", err)
			return
		}
	case strings.ToUpper(cfg.WorkMode) == "HTTP_DOWNLOAD_LIST":
		err := startHttpDownload(from, to)
		if err != nil {
			log.Println("Failed to download from list:", err)
			return
		}
	default:
		log.Fatal("ERR WorkMode, invalid FROM_URL or TO_URL")
	}
	log.Printf("\n\nTotalObjects:%d, TotalSizes:%s(%d). The program ran for %v\n", objectCount, ByteCountSI(sizeCount), sizeCount, time.Since(startTime))
	log.Println("From:", from.url)
	log.Println("To:", to.url)
}

// RetryRoundTripper wraps an http.RoundTripper with simple retry logic.
type RetryRoundTripper struct {
	Proxied http.RoundTripper
	Retries int
	Delay   time.Duration
}

func (rrt *RetryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error
	for i := 0; i <= rrt.Retries; i++ {
		resp, err = rrt.Proxied.RoundTrip(req)
		if err != nil {
			log.Printf("HTTP API Request failed and retry: %s", err)
			time.Sleep(rrt.Delay)
			continue
		}
		break
	}
	return resp, err
}
