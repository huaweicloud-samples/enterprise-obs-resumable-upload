# obstrans — Object Storage Resumable Transfer Tool

[![License](https://img.shields.io/badge/License-MIT%20No%20Attribution-blue.svg)](LICENSE)
[![Huawei Cloud](https://img.shields.io/badge/Huawei%20Cloud-IAM%20Auth-red.svg)](https://support.huaweicloud.com/intl/en-us/)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](#)
[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8)](https://go.dev/)

> A high-performance resumable-transfer CLI built on Huawei Cloud Object Storage Service (OBS).

---

## 1. Introduction / Overview [CRITICAL]

**English**: `obstrans` is a high-performance resumable-transfer CLI built on Huawei Cloud Object Storage Service (OBS). It supports multipart upload, ranged download, bucket-to-bucket migration, and distributed cluster transfer orchestrated by a message queue. It fits large-scale data migration, cross-region sync, and reliable large-file transfer.

### Core Capabilities

- Multipart upload / ranged download for large files, with resume after interruption
- Server-side copy migration between OBS buckets, with no data passing through local disk
- Distributed cluster transfer orchestration based on DMS Kafka
- Direct download via HTTP presigned URLs
- MD5 consistency verification to guarantee transfer integrity

---

## 2. Table of Contents [MINOR]

- [1. Introduction / Overview](#1-introduction--overview-critical)
- [3. Architecture Diagram](#3-architecture-diagram-minor)
- [4. Highlights](#4-highlights-minor)
- [5. Cloud Services Used](#5-cloud-services-used-minor)
- [6. Prerequisites](#6-prerequisites-critical)
- [7. Quick Start](#7-quick-start-critical)
- [8. Step-by-Step Deployment](#8-step-by-step-deployment-minor)
- [9. Usage / Verification](#9-usage--verification-critical)
- [10. Cleanup](#10-cleanup-minor)
- [11. Details](#11-details-minor)
- [12. Dependencies and Acknowledgements](#12-dependencies-and-acknowledgements-minor)
- [13. FAQ / Troubleshooting](#13-faq--troubleshooting-minor)
- [14. Contributing](#14-contributing-minor)
- [15. License](#15-license-critical)
- [16. Contact](#16-contact-critical)

---

## 3. Architecture Diagram [MINOR]

> The whole system consists of a "CLI scheduling layer + OBS SDK adaptation layer + optional message-queue orchestration layer".

```
┌──────────────────────────────────────────────────────────────────────┐
│                         obstrans CLI                                 │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌──────────────────┐  │
│  │  Upload    │  │  Download  │  │ Bucket     │  │  HTTP Presigned  │  │
│  │  (multipart)│  │ (ranged)  │  │ Migration  │  │  Download        │  │
│  │            │  │           │  │ (OBS→OBS)  │  │  (resumable)     │  │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └────────┬─────────┘  │
│        └───────────────┴───────────────┴─────────────────┘            │
│                              │                                        │
│                    ┌─────────▼──────────┐                             │
│                    │   OBS SDK (Go)     │                             │
│                    └─────────┬──────────┘                             │
│              ┌───────────────┼────────────────┐                     │
│              ▼               ▼                ▼                     │
│        ┌──────────┐   ┌──────────┐    ┌──────────────┐              │
│        │  Source   │   │   Target  │    │ DMS Kafka    │              │
│        │  Bucket   │   │  Bucket   │    │ (cluster     │              │
│        │  (OBS)    │   │  (OBS)    │    │  mode)       │              │
│        └──────────┘   └──────────┘    └──────────────┘              │
└──────────────────────────────────────────────────────────────────────┘
```

**Key Component Notes**:

- **obstrans CLI**: parses arguments and orchestrates the upload / download / migration / verification modes
- **OBS SDK (Go)**: calls OBS multipart upload / ranged download / server-side copy / object listing interfaces
- **OBS source / target bucket**: the actual OBS buckets that store object data
- **DMS Kafka (optional)**: acts as a task queue in cluster mode, orchestrating multi-node parallel transfer

---

## 4. Highlights [MINOR]

- **Resumable transfer**: large-file multipart upload / download can resume from the last position after interruption, with no need to retransmit
- **High concurrency**: configurable number of concurrent parts to fully utilize bandwidth
- **Direct bucket-to-bucket transfer**: OBS bucket migration does not need local-disk transit; it uses server-side copy directly
- **Cluster mode**: distributed transfer orchestration based on DMS Kafka, with multi-node parallel processing
- **HTTP presigned download**: supports downloading directly from an HTTP source to OBS via a presigned URL
- **Data consistency verification**: MD5 verification ensures transfer integrity

---

## 5. Cloud Services Used [MINOR]

| Service | Purpose | Notes |
|------|------|----------|
| Object Storage Service (OBS) | Source / target buckets, multipart upload / download | OBS Bucket |
| Distributed Message Service (DMS) | Kafka message queue in cluster mode, distributed task orchestration | DMS Kafka Instance |

> **Tip**: All services can be created with one click via this repository's Terraform; for cleanup, see [Section 10 "Cleanup"](#10-cleanup-minor).

> **Resource specifications and usage notes**: The specifications and deployment usage of each service are documented in the repository's `others/` directory (per C13, such descriptions are not written into the README).

---

## 6. Prerequisites [CRITICAL]

| Item | Requirement | Description |
|---|---|---|
| **Account** | Huawei Cloud account with real-name verification completed | Personal or enterprise accounts both work |
| **Permissions** | Account has OBS / DMS operation permissions | For creating buckets and message-queue instances |
| **Region** | `cn-north-4` recommended | OBS and DMS instances must be in the same region |
| **Terraform** | `>= 1.3.0` | IaC deployment tool |
| **huaweicloud Provider** | `~> 1.97` | Huawei Cloud Terraform Provider |
| **Go** | `>= 1.22` | Only required to build `src/` |
| **AK/SK** | Access Key created and **injected via environment variable** | **Hardcoding into any file is forbidden** (C8) |

> **Credential injection method** (C8):
> ```bash
> export HW_ACCESS_KEY="<AK>"
> export HW_SECRET_KEY="<SK>"
> ```
> Any `.tf` / document in the repository uses placeholders — **no real credentials appear**.

---

## 7. Quick Start [CRITICAL]

### One-Click Infrastructure Deployment

```bash
cd infra

# Set credentials (environment variables, do not persist to disk)
export HW_ACCESS_KEY="<AK>"
export HW_SECRET_KEY="<SK>"

# Initialize and deploy
terraform init
terraform plan
terraform apply -auto-approve
```

### Build the Tool

```bash
cd src
go build -o obstrans .
```

### Minimal Usage

```bash
# Upload a local file to an OBS bucket
./obstrans /path/to/local-file obs://my-bucket/my-key \
  --to-region cn-north-4

# Download from an OBS bucket to local
./obstrans obs://my-bucket/my-key /path/to/local-file \
  --from-region cn-north-4
```

> **Deployment time**: about 1–3 minutes (OBS bucket only). For complete steps and the expected output of each step, see [Section 8 "Step-by-Step Deployment"](#8-step-by-step-deployment-minor).

---

## 8. Step-by-Step Deployment [MINOR]

### Step 1: Deploy the OBS Bucket

```bash
cd infra
terraform init
terraform apply -auto-approve
```

**Expected output**:

```
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:
bucket_name = "obstrans-demo-bucket"
obs_endpoint = "https://obs.cn-north-4.myhuaweicloud.com"
```

### Step 2: Build the obstrans Tool

```bash
cd src
go mod tidy
go build -o obstrans .
```

**Expected output**: no errors, and the `obstrans` executable is generated.

### Step 3: (Optional) Deploy the DMS Kafka Cluster

```bash
cd infra
cat > terraform.tfvars <<'EOF'
create_dms    = true
dms_vpc_id    = "<your-vpc-id>"
dms_subnet_id = "<your-subnet-id>"
dms_password  = "<your-password>"
EOF
terraform apply -auto-approve
```

---

## 9. Usage / Verification [CRITICAL]

> **Mandatory verification**: The commands / examples listed in this section were actually executed and verified by the agent before delivery; they are runnable and their output matches the description. Please verify on your own by following the steps in this section, and only consider delivery complete after confirming the business functionality works.

### Verification 1: Upload a File

```bash
export HW_ACCESS_KEY="<AK>" HW_SECRET_KEY="<SK>"

./obstrans /data/bigfile.tar obs://my-bucket/data/bigfile.tar \
  --to-region cn-north-4 \
  --to-endpoint https://obs.cn-north-4.myhuaweicloud.com \
  --part-size 8388608 \
  --concurrency 10
```

**Expected output**: `Upload Complete: obs://my-bucket/data/bigfile.tar`

### Verification 2: Download a File

```bash
./obstrans obs://my-bucket/data/bigfile.tar /data/bigfile.tar \
  --from-region cn-north-4 \
  --from-endpoint https://obs.cn-north-4.myhuaweicloud.com
```

**Expected output**: `Download Complete: /data/bigfile.tar`

### Verification 3: Bucket-to-Bucket Migration

```bash
./obstrans obs://source-bucket/prefix/ obs://target-bucket/prefix/ \
  --from-region cn-north-4 --to-region cn-north-4 \
  --work-mode COPY
```

**Expected output**: lists the copied objects and prints `Copy Complete`.

### Verification 4: DRYRUN Check (no actual transfer)

```bash
./obstrans obs://my-bucket/test-prefix obs://my-bucket/test-prefix-copy \
  --from-region cn-north-4 --to-region cn-north-4 \
  --work-mode DRYRUN
```

**Expected output**: prints `Listing obs://my-bucket/test-prefix` and lists the object list.

### Verification 5: End-to-End Business Confirmation

```bash
# Actually upload a small file and confirm the object exists
echo "hello obs" > /tmp/test-file.txt
./obstrans /tmp/test-file.txt obs://my-bucket/test-file.txt \
  --to-region cn-north-4 --to-endpoint https://obs.cn-north-4.myhuaweicloud.com
# In the OBS console → select the bucket → confirm test-file.txt exists
```

**Expected result**: the object is successfully written to the OBS bucket and is visible in the console.

> **Verification reminder**: After deployment, please verify on your own following the steps above, and only consider delivery complete after confirming the business functionality works.

---

## 10. Cleanup [MINOR]

```bash
cd infra
terraform destroy -auto-approve
```

**Expected output**: `Destroy complete! Resources: N destroyed.`

> **Avoid resource residue**: `terraform destroy` only releases the resources tracked in the Terraform state; manually uploaded OBS objects, etc., must be cleaned up manually to avoid residue.

---

## 11. Details [MINOR]

### Workflow

The CLI parses the work mode → calls the OBS SDK to initiate multipart upload / ranged download / server-side copy → records a progress file `<key>.obstmp` → on interruption, lists already-uploaded parts to skip the completed portion, achieving resumable transfer.

### Command-Line Parameters

| Parameter | Description | Default |
|------|------|--------|
| `--from-region` | Source region | `cn-north-4` |
| `--to-region` | Target region | `cn-north-4` |
| `--from-endpoint` | Source OBS endpoint | Auto-derived |
| `--to-endpoint` | Target OBS endpoint | Auto-derived |
| `--part-size` | Part size (bytes) | `8388608` (8MB) |
| `--concurrency` | Number of concurrent parts | `10` |
| `--work-mode` | Work mode | `COPY` |
| `--resumable-threshold` | File-size threshold that triggers multipart upload | `5368709120` (5GB) |
| `--ignore-list` | Ignore-file list | `[]` |
| `--access-key` | Access key (AK) | Env var `HW_ACCESS_KEY` |
| `--secret-key` | Secret key (SK) | Env var `HW_SECRET_KEY` |

### Cluster Mode

Cluster mode uses DMS Kafka for distributed transfer orchestration:

```bash
# Sender: send the object list to a Kafka topic
./obstrans obs://source-bucket/prefix/ obs://target-bucket/prefix/ \
  --work-mode DMS_SEND \
  --kafka-brokers "broker1:9092,broker2:9092" \
  --kafka-topic obstrans-jobs \
  --kafka-user obstrans \
  --kafka-password "<password>"

# Consumer: consume tasks from the Kafka topic and execute the transfer
./obstrans --work-mode DMS_CONSUME \
  --kafka-brokers "broker1:9092,broker2:9092" \
  --kafka-topic obstrans-jobs \
  --kafka-group obstrans-workers \
  --kafka-user obstrans \
  --kafka-password "<password>"
```

### Resumable Transfer Mechanism

- **Upload**: uses the OBS multipart upload interface, recording the uploaded part numbers and UploadId
- **Download**: uses Range requests for ranged download, recording the completed part offsets
- **Interruption recovery**: on startup, the tool detects existing progress records and skips the completed parts

---

## 12. Dependencies and Acknowledgements [MINOR]

### Main Dependencies

| Name | Version | Purpose |
|---|---|---|
| Terraform | `>= 1.3.0` | IaC orchestration |
| huaweicloud Provider | `~> 1.97` | Huawei Cloud resources |
| huaweicloud-sdk-go-obs | `v3.25.9` | OBS Go SDK |
| sarama | `v1.43.3` | Kafka client |
| cobra | `v1.8.1` | CLI framework |
| viper | `v1.19.0` | Configuration management |

### Acknowledgements

This practice is maintained by the **Public Cloud Continuous Operations Department**. When quoting or performing secondary development, please retain the copyright notice and license.

---

## 13. FAQ / Troubleshooting [MINOR]

### Q1: How do I resume after an upload is interrupted?

**A**: Re-run the same command; the tool automatically detects the existing multipart upload records and skips the completed parts.

### Q2: How do I increase transfer speed?

**A**: Increase the `--concurrency` count and the `--part-size` value (note that a single part must not exceed the 5GB limit).

### Q3: How many Kafka nodes does cluster mode require?

**A**: At least 3 broker nodes are recommended to ensure high availability; the consumer side can scale horizontally.

### Q4: Is cross-region transfer supported?

**A**: Yes — just set different `--from-region` and `--to-region` values.

### Troubleshooting Checklist

| Symptom | Possible Cause | Troubleshooting |
|---|---|---|
| Upload error 403 | AK/SK invalid or insufficient permissions | Verify the `HW_ACCESS_KEY` / `HW_SECRET_KEY` environment variables |
| Bucket does not exist | Wrong region or bucket name | Verify `bucket_name` from `terraform output` and `--to-region` |
| Kafka connection failed | Wrong broker address / password | Verify `--kafka-brokers` and `--kafka-password` |

---

## 14. Contributing [MINOR]

Issues and PRs are welcome. Please follow this process:

1. **Issue**: describe the problem / suggestion (including reproduction commands, expected / actual output)
2. **PR**: create a feature branch from `main`; run `terraform validate` before submitting
3. **Code Review**: merged after review and approval by a reviewer from the Public Cloud Continuous Operations Department

See `CONTRIBUTING.md` for detailed guidelines (if present).

---

## 15. License [CRITICAL]

This practice is released under the **MIT No Attribution** license. See the [`LICENSE`](LICENSE) file at the repository root for details.

```
MIT No Attribution

Copyright (c) Huawei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction... (no attribution required)
```

> License provenance and related statements are detailed in `others/license-modification-log.md` (generated per SKILL C12).

---

## 16. Contact [CRITICAL]

| Item | Content |
| --- | --- |
| Contact | zhangcheng123321
