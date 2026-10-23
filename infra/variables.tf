# ============================================================
# 变量（凭据一律通过环境变量注入，不写死）
# ============================================================
variable "region" {
  description = "华为云区域，例如 cn-north-4"
  type        = string
  default     = "cn-north-4"
}

variable "bucket_name" {
  description = "对象存储桶名称（全局唯一）"
  type        = string
  default     = "obstrans-demo-bucket"
}

variable "storage_class" {
  description = "桶存储类别：STANDARD / WARM / COLD"
  type        = string
  default     = "STANDARD"
}

variable "bucket_acl" {
  description = "桶 ACL：private / public-read / public-read-write / authenticated-read / bucket-owner-full-control"
  type        = string
  default     = "private"
}

# ---- 分布式集群模式（DMS Kafka，集群消息队列能力）----
variable "create_dms" {
  description = "是否创建 DMS Kafka 实例用于分布式传输编排"
  type        = bool
  default     = false
}

variable "dms_name" {
  description = "DMS Kafka 实例名称"
  type        = string
  default     = "obstrans-dms"
}

variable "dms_vpc_id" {
  description = "DMS Kafka 实例所在 VPC ID"
  type        = string
  default     = ""
}

variable "dms_subnet_id" {
  description = "DMS Kafka 实例所在子网（network）ID"
  type        = string
  default     = ""
}

variable "dms_az" {
  description = "DMS Kafka 实例可用区名称，例如 cn-north-4a"
  type        = string
  default     = "cn-north-4a"
}

variable "dms_password" {
  description = "DMS Kafka 实例 SASL 密码（通过环境变量或 terraform.tfvars 注入，勿落盘明文）"
  type        = string
  default     = ""
  sensitive   = true
}
