from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
import pyarrow as pa
import polars as pl

BROKERS = ["localhost:19092"]
TOPICS = ["arrow_tables"]

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=BROKERS, topics=TOPICS)
op.inspect("inspect-errors", kinp.errs)
tables = op.map("tables", kinp.oks, lambda msg: pl.from_arrow(pa.ipc.open_file(msg.value).read_all()))

# get the max value
max_cpu = op.map("tables", kinp.oks, lambda df: df.select(pl.col("cpu_used").max()))
op.inspect("max_cpu", max_cpu)