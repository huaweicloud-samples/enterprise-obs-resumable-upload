# ============================================================
# 输出
# ============================================================
output "bucket_name" {
  description = "对象存储桶名称"
  value       = huaweicloud_obs_bucket.store.bucket
}

output "bucket_domain_name" {
  description = "对象存储桶访问域名"
  value       = huaweicloud_obs_bucket.store.bucket_domain_name
}

output "obs_endpoint" {
  description = "OBS 区域终端节点，供 obstrans 的 --to-endpoint / --from-endpoint 使用"
  value       = "https://obs.${var.region}.myhuaweicloud.com"
}

output "dms_brokers" {
  description = "DMS Kafka 实例连接地址（create_dms=true 时可用）"
  value       = var.create_dms ? huaweicloud_dms_kafka_instance.transfer[0].connect_address : ""
}
