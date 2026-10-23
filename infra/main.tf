# ============================================================
# 华为云 provider（凭据通过环境变量 HW_ACCESS_KEY / HW_SECRET_KEY 注入）
# ============================================================
provider "huaweicloud" {
  region = var.region
}

# ------------------------------------------------------------
# 安全组：DMS Kafka 实例网络隔离（仅集群模式需要）
# ------------------------------------------------------------
resource "huaweicloud_networking_secgroup" "dms" {
  count       = var.create_dms ? 1 : 0
  name        = "${var.dms_name}-sg"
  description = "Security group for DMS Kafka instance used by obstrans"
  delete_default_rules = true
}

resource "huaweicloud_networking_secgroup_rule" "kafka_allow" {
  count             = var.create_dms ? 1 : 0
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 9092
  port_range_max    = 9094
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = huaweicloud_networking_secgroup.dms[0].id
}

# ------------------------------------------------------------
# 对象存储桶：断点续传工具的源/目标存储
# ------------------------------------------------------------
resource "huaweicloud_obs_bucket" "store" {
  bucket        = var.bucket_name
  storage_class = var.storage_class
  acl           = var.bucket_acl
  # 调试/示例桶便于一键销毁，正式桶请按需关闭
  force_destroy = true

  tags = {
    usage = "obstrans-resumable-transfer"
  }
}

# ------------------------------------------------------------
# DMS Kafka 实例（可选）：用于分布式传输编排
# 对应基于消息队列的集群模式
# 仅在 create_dms = true 时创建，用于分布式传输编排
# ------------------------------------------------------------
resource "huaweicloud_dms_kafka_instance" "transfer" {
  count              = var.create_dms ? 1 : 0
  name               = var.dms_name
  engine_version     = "2.3.0"
  broker_num         = 3
  flavor_id          = "c3.2u.large.2"
  storage_spec_code  = "dms.physical.storage.high"
  storage_space      = 600
  access_user        = "obstrans"
  password           = var.dms_password
  vpc_id             = var.dms_vpc_id
  network_id         = var.dms_subnet_id
  security_group_id  = huaweicloud_networking_secgroup.dms[0].id
  security_protocol  = "SASL_SSL"
  available_zones    = [var.dms_az]
  charging_mode      = "postPaid"
}
