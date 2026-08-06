# Oxidant quickstart (standalone AMI)

This instance runs the Oxidant Spark Connect server as `oxidant-standalone.service`
(systemd). Oxidant speaks the Apache Spark Connect protocol, so stock PySpark /
Spark SQL clients work unmodified — no JVM anywhere.

## 1. Check the server

```bash
systemctl status oxidant-standalone
journalctl -u oxidant-standalone -f        # logs
cat /etc/oxidant/VERSION                   # engine build stamp
```

## 2. Connect from PySpark (any machine that can reach port 50051)

```bash
pip install "pyspark-client>=4.0"          # pure-Python, no JVM
```

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://<host>:50051").getOrCreate()
spark.sql("SELECT 1 AS hello").show()
```

Open only TCP 50051 (and SSH 22) in your security group. The Connect protocol
is unauthenticated — restrict ingress to your client CIDRs.

## 3. Query your data on S3

```sql
CREATE VIEW hits AS SELECT * FROM parquet.`s3://<bucket>/<prefix>/`;
SELECT count(*) FROM hits;
```

For AWS Glue catalogs, pass catalog config at startup (see
`OXIDANT_CATALOG_CONF` in `docs/catalogs.md` in the repo) — the instance
profile's IAM role is used for credentials.

## 4. Monitoring UI

The UI binds loopback for safety. Tunnel in:

```bash
ssh -L 4040:localhost:4040 ec2-user@<host>
# open http://localhost:4040
```

## 5. Scale out (optional)

For a driver + N workers cluster, use the CloudFormation template in the repo
(`deploy/cloudformation/oxidant-cluster.yaml`) or tag instances per
`docs/distributed-ec2.md`. Untagged instances always boot standalone.

## Support

- Issues: https://github.com/OxidantData/Oxidant/issues
- Commercial support / licensing: hello@oxidantdata.com
