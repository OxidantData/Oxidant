# AWS Glue Data Catalog

Oxidant reads the AWS Glue Data Catalog through its external-catalog SPI: tables already
registered in Glue resolve as three-part names `glue.<database>.<table>`, loaded lazily on
first reference. For Hive Metastore and Iceberg REST/Unity catalogs, see
[`catalogs.md`](catalogs.md); for the full EC2/ASG walkthrough (IAM stack parameters, S3
setup), see [`distributed-ec2.md`](distributed-ec2.md).

> **Glue is optional.** Without any `--catalog-conf`, the engine serves its built-in local
> catalog (`spark_catalog`, current database `default`) — an in-memory catalog where
> `CREATE TABLE`/`CREATE EXTERNAL TABLE` land, plus the bundled `samples` schema when the
> server is started with `--sample-data` (see [getting-started.md](getting-started.md)). No
> cloud account or metastore is needed to query data.

## How auth works

Oxidant's Glue provider (`oxidant-catalog-glue`) does not use an AWS SDK directly — it **shells
out to the AWS CLI** (`aws glue get-databases|get-tables|get-table …`), and table locations
(`s3://…`) are read with the same credentials. So any identity the AWS CLI can use works:

- **EC2:** an IAM instance profile, picked up via IMDSv2 — no static keys anywhere.
- **Anywhere else:** standard AWS credential environment variables
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`) or a configured CLI
  profile.

The CLI binary path is taken only from `OXIDANT_AWS_BIN` (never from catalog options). The
Docker image and the EC2 AMI already bundle AWS CLI v2 and set this; on a bare-metal install,
`aws` must be on `PATH` or set `OXIDANT_AWS_BIN=/path/to/aws`.

**Region precedence:** the `spark.sql.catalog.glue.region` option → `AWS_REGION` →
`AWS_DEFAULT_REGION` → `us-west-2`.

## Configure

At server start, with repeated `--catalog-conf` flags:

```sh
oxidant spark server --port 50051 \
  --catalog-conf spark.sql.catalog.glue.type=glue \
  --catalog-conf spark.sql.catalog.glue.region=us-east-1 \
  --catalog-conf spark.sql.catalog.glue.warehouse=s3://bucket/prefix
```

Or with one env var (`;`-separated):

```sh
export OXIDANT_CATALOG_CONF="spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=us-east-1;spark.sql.catalog.glue.warehouse=s3://bucket/prefix"
oxidant spark server --port 50051
```

Or per session, from a Spark Connect client:

```python
spark.conf.set("spark.sql.catalog.glue.type", "glue")
spark.conf.set("spark.sql.catalog.glue.region", "us-east-1")
spark.conf.set("spark.sql.catalog.glue.warehouse", "s3://bucket/prefix")
```

| Option | Required | Purpose |
|--------|----------|---------|
| `spark.sql.catalog.glue.type` | yes | Must be `glue` |
| `spark.sql.catalog.glue.region` | recommended | Glue/S3 region (see precedence above) |
| `spark.sql.catalog.glue.warehouse` | for CTAS | Default `s3://bucket/prefix` root for `CREATE TABLE AS` without `LOCATION` |

**Distributed:** pass the same `--catalog-conf` / `OXIDANT_CATALOG_CONF` to every
`oxidant worker` — workers resolve Glue locations themselves when they execute stages.
Session-level config (the Python option above) registers the catalog on the driver only.

## Verify

```sql
SHOW DATABASES IN glue;
SHOW TABLES IN glue.oxidant_demo;
SELECT count(*) AS n FROM glue.oxidant_demo.orders;
SELECT * FROM glue.oxidant_demo.orders LIMIT 10;
```

Or from PySpark:

```python
spark.catalog.listDatabases()
spark.catalog.listTables("oxidant_demo")
spark.catalog.tableExists("glue.oxidant_demo.orders")
```

Use fully-qualified `glue.<database>.<table>` names — unqualified names do not yet resolve
through external catalogs (current limitations: [`catalogs.md`](catalogs.md)).

## CTAS and the warehouse option

`CREATE TABLE glue.db.t AS SELECT …` needs an S3 location. Either give one explicitly with
`LOCATION 's3://…'`, or set `spark.sql.catalog.glue.warehouse` once and CTAS places the new
table under that root. Without a `warehouse` and without `LOCATION`, CTAS fails — plain
reads don't need `warehouse` at all.

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `aws: command not found` / spawn error | No AWS CLI where the server runs. Install AWS CLI v2, or set `OXIDANT_AWS_BIN=/path/to/aws` |
| `aws glue … EntityNotFound` | Wrong database/table name, or wrong region — set `spark.sql.catalog.glue.region` explicitly |
| `AccessDenied` on Glue or S3 | The identity (instance profile / env creds) lacks `glue:Get*` or S3 read on the bucket **and** `bucket/*`; on EC2 check the instance profile |
| Auth works locally but not from Oxidant | Credentials must be visible to the **server** process (or every worker), not your client shell |
| Works single-node, fails distributed | Workers missing the catalog — pass the same `OXIDANT_CATALOG_CONF` to `oxidant worker` |
| CTAS fails with missing location | Set `spark.sql.catalog.glue.warehouse` or add `LOCATION 's3://…'` |

Sanity-check identity and reachability from the host Oxidant runs on:

```sh
aws sts get-caller-identity
aws glue get-databases --region us-east-1
```
