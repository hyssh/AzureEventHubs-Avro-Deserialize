# AzureEventHubs-Avro-Deserialize [Python]

This notebook sample will show you how to use SchemaRegistry to deserialize Avro messages with Spark Structured Streaming query.

You can find the code from [Notebook](https://github.com/hyssh/AzureEventHubs-Avro-Deserialize/blob/main/py_EH_Avro_deserialize_body.ipynb)


## Import library

```python
import os
from azure.identity import DefaultAzureCredential
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.serializer.avroserializer import AvroSerializer
```


## Get Environment variables

```
os.environ["AZURE_CLIENT_ID"] = spark.sparkContext.environment.get("AZURE_CLIENT_ID")
os.environ["AZURE_TENANT_ID"] = spark.sparkContext.environment.get("AZURE_TENANT_ID")
os.environ["AZURE_CLIENT_SECRET"] = spark.sparkContext.environment.get("AZURE_CLIENT_SECRET")
os.environ["EVENT_HUB_CONN_STR"] = spark.sparkContext.environment.get("EVENT_HUB_CONN_STR")
os.environ["EVENT_HUB_CONN_STR_ENT"] = spark.sparkContext.environment.get("EVENT_HUB_CONN_STR_ENT")
os.environ["EVENT_HUB_CONN_STR_LISTEN"] = spark.sparkContext.environment.get("EVENT_HUB_CONN_STR_LISTEN")

SCHEMAREGISTRY_FULLY_QUALIFIED_NAMESPACE = "shin-eventhub-ns.servicebus.windows.net"
EVENTHUB_NAME="transactions"
```


## Create UDF

```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def deserializeBody(encodedBody):    

    token_credential = DefaultAzureCredential()

    schema_registry_client = SchemaRegistryClient("shin-eventhub-ns.servicebus.windows.net", token_credential)
    avro_serializer = AvroSerializer(client=schema_registry_client, group_name="tranxs")

    return avro_serializer.deserialize(encodedBody)

deserializedBody_udf = udf(deserializeBody, StringType())
```


## Connect to Event Hubs

```
ehConf = {}

# For versions before 2.3.15, set the connection string without encryption
# ehConf['eventhubs.connectionString'] = os.environ["EVENT_HUB_CONN_STR"]

# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(os.environ["EVENT_HUB_CONN_STR_ENT"])

# Confrim the consumer group from Event Hub
# https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-features#consumer-groups
ehConf['eventhubs.consumerGroup'] = "spark"
```


## Read streaming data from Event Hubs

```python
# Simple batch query
df = spark.readStream.format("eventhubs").options(**ehConf).load()

# df = df.outputMode("append").format("console").start().awaitTermination()
df.printSchema()
```


## Write stream data after deserialize Avro message

```python
ds4 = df.withColumn("decodedBody", deserializedBody_udf(df["body"])).writeStream.format("parquet")\
    .option("path", "abfss://dev-synapse@hyundevsynapsestorage.dfs.core.windows.net/streamingAfterdecode")\
    .option("checkpointLocation", "abfss://dev-synapse@hyundevsynapsestorage.dfs.core.windows.net/streamingAfterdecode_checkpoint")\
    .start()
```
