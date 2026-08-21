# pyoxidant

Launches a [Oxidant](https://github.com/OxidantData/Oxidant) Spark Connect server. Bring your own
stock PySpark client and change one line:

```python
from pyoxidant import SparkConnectServer
SparkConnectServer(port=50051).start()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*) FROM parquet.`hits.parquet`").show()
```

```sh
pip install "pyoxidant[client]"
```
