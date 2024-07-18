# Streaming Arrow

<img width="895" alt="streaming_arrow" src="https://github.com/bytewax/streaming-arrow/assets/6073079/227c9577-4467-41c5-9efa-8ba27b1b5e59">

A complete streaming architecture with Bytewax, ClickHouse and Redpanda designed around Arrow.

To see more details on the why, check out the [blog post]().

## Running

Start clickhouse and redpanda with docker compose

```
$ docker compose up -d
```

Run the data generator

```
$ python -m bytewax.run src/input.py
```

Run the output

```
$ python -m bytewax.run src/output.py
```

inspect the table

```
$ docker exec -it clickhouse clickhouse-client
clickhouse :) select * from bytewax.compstat
```

## Explanation of the Bytewax Code

Bytewax is a Python stateful stream processor that is exceptionally flexible in it's ability to integrate with other data tools and infrastructure.

We have 5 Python files that use Bytewax

## Connectors

We have two files that are custom connectors that allow you to write arrow out to different Sinks.
* `adbc_connect.py`
* `clickhouse_connector.py`

We won't go into depth around how these connectors function. They take pyarrow tables and write them to a clickhouse database.

## Dataflows

We have three files that are Bytewax dataflows. These are piplines.

* `input.py` - capture streaming data
* `output.py` - write data to datastore with Sink connector
* `analyze.py` - analyze the streaming data

These are examples of how you would build dataflows to manage the streaming data within your organization using Bytewax. 

### Input.py

The input dataflow is using psutil to generate a datastream for us and writes it to Redpanda or Kafka with the Bytewax built-in Kafka connector.

```python
import pyarrow as pa
from bytewax.connectors.kafka import KafkaSinkMessage, operators as kop
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from datetime import datetime
from time import perf_counter
import psutil


BROKERS = ["localhost:19092"]
TOPIC = "arrow_tables"

run_start = perf_counter()

SCHEMA = pa.schema([
    ('device',pa.string()),
    ('ts',pa.timestamp('us')), # microsecond
    ('cpu_used',pa.float32()),
    ('cpu_free',pa.float32()),
    ('memory_used',pa.float32()),
    ('memory_free',pa.float32()),
    ('run_elapsed_ms',pa.int32()),
])

def sample_wide_event():
    return {
        'device':'localhost',
        'ts': datetime.now(),
        'cpu_used': psutil.cpu_percent(),
        'cpu_free': round(1 - (psutil.cpu_percent()/100),2)*100,
        'memory_used': psutil.virtual_memory()[2], 
        'memory_free': round(1 - (psutil.virtual_memory()[2]/100),2)*100, 
        'run_elapsed_ms': int((perf_counter() - run_start)*1000)
    }

def sample_batch_wide_table(n):
    samples = [sample_wide_event() for i in range(n)]
    arrays = []
    for f in SCHEMA:
        array = pa.array([samples[i][f.name] for i in range(n)], f.type)
        arrays.append(array)
    t = pa.Table.from_arrays(arrays, schema=SCHEMA)

    return t

def table_to_compressed_buffer(t: pa.Table) -> pa.Buffer:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(
        sink,
        t.schema,
        options=pa.ipc.IpcWriteOptions(
            compression=pa.Codec(compression="zstd", compression_level=1)
        ),
    ) as writer:
        writer.write_table(t)
    return sink.getvalue()

BATCH_SIZE = 1000
N_BATCHES = 10
table_gen = (sample_batch_wide_table(BATCH_SIZE) for i in range(N_BATCHES))

flow = Dataflow("arrow_producer")

# this is only an example. should use window or collect downstream
tables = op.input("tables", flow, TestingSource(table_gen))

buffers = op.map("string_output", tables, table_to_compressed_buffer)
messages = op.map("map", buffers, lambda x: KafkaSinkMessage(key=None, value=x))
op.inspect("message_stat_strings", messages)
kop.output("kafka_out", messages, brokers=BROKERS, topic=TOPIC)
```

#### Output.py

The Output pipeline uses Bytewax to write data from Kafka to Clickhouse with a custom connector defined in `src/clickhouse_connector`. This is a proof of concept and doesn't contain the guarantees required for production system. Reach out to the bytewax team @ sales@bytewax.io for details on the premium connector. 

```python
from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
import pyarrow as pa

from clickhouse_connector import ClickhouseSink

SCHEMA = """
        device String,
        ts DateTime,
        cpu_used Float64,
        cpu_free Float64,
        memory_used Float64,
        memory_free Float64,
        run_elapsed_ms Int64
        """

ORDER_BY = "device, ts"

BROKERS = ["localhost:19092"]
TOPICS = ["arrow_tables"]

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=BROKERS, topics=TOPICS)
op.inspect("inspect-errors", kinp.errs)

tables = op.map("tables", kinp.oks, lambda msg: pa.ipc.open_file(msg.value).read_all())
op.inspect("message_stat_strings", tables, lambda _sid, t: f"step_is: {_sid} has data: {t.shape} {t['ts'][0]}")
op.output("output_clickhouse", tables, ClickhouseSink("compstat", "admin", "password", database="bytewax", port=8123, schema=SCHEMA, order_by=ORDER_BY))
```