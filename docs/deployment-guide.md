# 部署指导书（obstrans）

> 本文档描述 `obstrans` 对象存储断点续传工具的一键部署与编译流程。所有 `cd` 均使用相对路径，凭据通过环境变量注入，不落盘。

## 1. 前置准备

| 项 | 要求 |
|---|---|
| 华为云账号 | 已开通并完成实名认证，具备 OBS / DMS 操作权限 |
| 访问密钥 | 已创建 AK/SK，通过环境变量注入（见下文） |
| Go 工具链 | Go 1.22 及以上（仅编译 `src/` 时需要） |
| Terraform | 1.3 及以上（部署基础设施） |
| 区域 | OBS 与 DMS 实例需位于同一区域（如 `cn-north-4`） |

**凭据注入（C8）**

```bash
export HW_ACCESS_KEY="<AK>"
export HW_SECRET_KEY="<SK>"
# 集群模式（create_dms=true）时还需：
export TF_VAR_dms_password="<DMS_PASSWORD>"
```

## 2. 部署基础设施（OBS 桶）

```bash
cd infra
terraform init
terraform plan
terraform apply -auto-approve
```

**预期输出**

```
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:
bucket_name = "obstrans-demo-bucket"
obs_endpoint = "https://obs.cn-north-4.myhuaweicloud.com"
```

`terraform output` 会列出 `bucket_name`、`bucket_domain_name`、`obs_endpoint` 等关键值，请妥善保存用于后续验证。

## 3. 编译 obstrans 工具

```bash
cd src
go mod tidy
go build -o obstrans .
```

**预期输出**：无报错，生成 `obstrans` 可执行文件。

## 4. （可选）部署 DMS Kafka 集群

仅在需要分布式传输编排时执行：

```bash
cd infra
cat > terraform.tfvars <<'EOF'
create_dms   = true
dms_vpc_id   = "<your-vpc-id>"
dms_subnet_id = "<your-subnet-id>"
dms_password = "<your-password>"
EOF
terraform apply -auto-approve
```

> 说明：DMS 实例为可选资源，创建前请在对话 / `others/` 中确认规格与用量；本文档不列相关用量说明（C13）。

## 5. 快速验证

```bash
# 确认编译成功
./obstrans -h

# DRYRUN 模式（仅列举，不实际传输）
export HW_ACCESS_KEY="<AK>" HW_SECRET_KEY="<SK>"
./obstrans obs://my-bucket/test-prefix obs://my-bucket/test-prefix-copy \
  --from-region cn-north-4 --to-region cn-north-4 \
  --work-mode DRYRUN
```

详细验证方法见仓库根目录 `README.md` 第 9 节「使用方法 / 验证」。

## 6. 释放资源

```bash
cd infra
terraform destroy -auto-approve
```

> 该操作释放 Terraform state 中追踪的资源，避免残留。手工创建的 OBS 对象等需另行清理。
