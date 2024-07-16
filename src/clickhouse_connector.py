from typing import Any, List
from typing_extensions import override
import logging

from bytewax.outputs import StatelessSinkPartition, DynamicSink
from pyarrow import Table
from clickhouse_connect import Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    
class _ClickHousePartition(StatelessSinkPartition):
    def __init__(self, table_name, host, port, username, password, database):
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = Client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
    
    @override
    def write_batch(self, batch: List[Table]) -> None:
        for table in batch:
            arrow_buffer = table.serialize()
            self.client.insert_arrow(self.table_name, arrow_buffer)


class ClickhouseSink(DynamicSink):
    def __init__(self, table_name, host, port, username, password, database, schema=None, order_by_fields=''):
        self.table_name = table_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema

        # init client
        client = Client(host=self.host, port=self.port, username=self.username, password=self.password, database=self.database)
        
        # Check if the table exists
        table_exists_query = f"SELECT name FROM system.tables WHERE database = {self.database} AND name = '{self.table_name}'"
        table_exists = client.query(table_exists_query)

        if not table_exists:
            logger.info(f"Table '{self.table_name}' does not exist.")
            if schema:
                # Create the table with ReplacingMergeTree
                create_table_query = f"""
                CREATE TABLE {table_name} (
                    {self.schema}
                ) ENGINE = ReplacingMergeTree()
                ORDER BY tuple()
                """
                client.command(create_table_query)
                logger.info(f"Table '{table_name}' created successfully.")
            else:
                raise("""Can't complete execution without schema of format
                        column1 UInt32,
                        column2 String,
                        column3 Date""")
        else:
            logger.info(f"Table '{self.table_name}' exists.")

            # Check the MergeTree type
            mergetree_type_query = f"SELECT engine_full FROM system.tables WHERE database = {self.database} AND name = '{self.table_name}'"
            mergetree_type_result = client.query(mergetree_type_query)
            mergetree_type = mergetree_type_result['engine_full'][0]
            logger.info(f"MergeTree type of the table '{table_name}': {mergetree_type}")

            if "ReplacingMergeTree" not in mergetree_type:
                logger.warning(f"""Table '{table_name}' is not using ReplacingMergeTree. 
                               Consider modifying the table to avoid duplicates on restart""")

            # Get the table schema
            schema_query = f"""
            SELECT name, type FROM system.columns
            WHERE database = 'your_database' AND table = '{table_name}'
            """
            schema_result = client.query(schema_query)

            logger.info(f"Schema of the table '{table_name}':")
            for column in schema_result:
                logger.info(f"Column: {column['name']}, Type: {column['type']}")
        
        client.close()

    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> _ClickHousePartition:
        return _ClickHousePartition(self.table_name, self.host, self.port, self.username, self.password, self.database)