# Oxidant on AWS Marketplace

Oxidant is distributed as a **paid hourly AMI** on AWS Marketplace: a customer
subscribes, launches an EC2 instance, and gets a running Spark Connect server
with zero configuration. AWS handles all metering and billing alongside the
customer's EC2 usage; there is no license-key machinery.

This page is the publishing runbook (seller side) plus the customer flow the
listing describes.

## What the AMI does on first boot

`oxidant-bootstrap.service` reads the instance tags:

- **No tags (the Marketplace path)** → *standalone*: `oxidant-standalone.service`
  runs `oxidant spark server --port 50051 --ui-bind 127.0.0.1`. The customer
  points any PySpark client at `sc://<host>:50051`. The monitoring UI is
  loopback-only (no auth) and reachable via SSH tunnel.
- **`oxidant:role=driver|worker` + cluster tags** → the driver/worker cluster
  path documented in [`distributed-ec2.md`](distributed-ec2.md) (same AMI,
  used by `deploy/cloudformation/oxidant-cluster.yaml`).

The image is AL2023-based, IMDSv2-only, SSH password-auth off, root login off,
`dnf-automatic` security updates on, engine runs as an unprivileged `oxidant`
user under a locked-down systemd unit, and nothing phones home. Build stamp in
`/etc/oxidant/VERSION` + the `EngineVersion` AMI tag.

## Customer flow (listing copy outline)

1. Subscribe → launch (recommended: `r6i.xlarge` / `r7g.xlarge` or larger;
   single node handles interactive analytics on S3 directly).
2. Security group: inbound TCP 50051 (Spark Connect) from your client CIDRs,
   TCP 22 for SSH/SSM. No other ports needed.
3. `pip install "pyspark-client>=4.0"`, then
   `SparkSession.builder.remote("sc://<host>:50051")` — unmodified PySpark
   works; no JVM.
4. Query S3 directly (`parquet.`s3://…`` views) or attach a Glue catalog
   (`docs/catalogs.md`). Scale out later with the CloudFormation template.

Verified performance of the engine this AMI ships (SF10, 3-node r6g-class
cluster, warm S3 disk cache): **TPC-DS 99/99 queries 80% faster than Apache
Spark on EMR (56.4s vs 282.8s)** and **TPC-H 22/22 82% faster (15.3s vs
87.3s)**. Cache-disabled (cold S3 streaming) the engine remains ~10–20%
faster. Methodology + per-query matrix:
https://oxidantdata.com/#/performance

## Seller publishing runbook

One-time:

1. **Seller registration** (AWS Marketplace Management Portal): business
   entity, tax (W-9/W-8), bank account for disbursements. Confirm the current
   listing-fee percentage in the portal — do not quote stale numbers.
2. Register `hello@oxidantdata.com` as the support contact (mailbox must
   actually exist — set it up before listing).
3. Trademark clearance/filing for "Oxidant" (policy: `TRADEMARK.md`).

Per release:

1. Build the engine for **both** `x86_64` and `arm64` (Marketplace AMI
   products can offer multiple architectures; build native on AL2023 —
   `docs/distributed-ec2.md` has the recipe).
2. `GIT_SHA=$(git rev-parse --short HEAD) ./deploy/packer/build-ami.sh
   --binary <path> --arch <arch>` — build in **us-east-1** (Marketplace
   ingestion region), note the AMI ids.
3. Self-scan before submission: no default credentials/keys, SSH key-only,
   all updates applied at bake time, minimal listening ports (50051, 22),
   no outbound telemetry. The provisioner already enforces this; verify with
   a fresh boot + `ss -tlnp` + the standalone smoke below.
4. Submit the AMI product in the Management Portal (product code is attached
   by AWS), supply listing copy (above), EULA (use the Commercial License
   from `COMMERCIAL.md`; the AMI is the commercial distribution — AGPLv3
   source remains on GitHub), and the hourly price.
   - Pricing anchor: EMR Serverless / Databricks charge a multiple of the
     EC2 cost. A $0.30–0.50/hr per-instance markup on large instances is a
     strong value story against the verified 4–5× speedups; validate at
     listing time.
5. AWS runs the AMI scan (CVEs, auth posture, port review) — fix findings,
   resubmit.
6. **Test-subscribe from a second AWS account**, launch, and run the
   standalone smoke test below before making the version public.
7. Publish, then roll new regions in from us-east-1.

Standalone smoke test (run on every candidate AMI before submission):

```bash
aws ec2 run-instances --image-id <ami> --instance-type r6i.xlarge \
  --key-name <key> --security-group-ids <sg-with-22+50051>   # no tags
# wait ~2 min, then:
ssh ec2-user@<ip> 'systemctl is-active oxidant-standalone && cat /etc/oxidant/VERSION'
python3 -c "from pyspark.sql import SparkSession; \
  SparkSession.builder.remote('sc://<ip>:50051').getOrCreate() \
    .sql('SELECT 1').show()"     # needs pyspark-client
```

## Obligations after listing

- **Patch cadence**: rebuild the AMI on engine releases and for base-OS CVE
  waves (AWS notifies on scan findings for published AMIs; stale images get
  delisted). Track Rust-side CVEs with `cargo audit` / `cargo deny`
  (policy in `deny.toml`).
- **Version lifecycle**: Marketplace versions map 1:1 to AMI ids; deprecate
  old versions when a new one publishes (customers on deprecated versions
  keep running but can't launch new instances).
- **Support**: Marketplace requires a published support channel —
  hello@oxidantdata.com + the GitHub issue tracker.
