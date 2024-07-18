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