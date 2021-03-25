from typing import Optional
from datetime import timedelta
from pyflink.common.typeinfo import TypeInformation
from pyflink.common.serialization import JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import Table, StreamTableEnvironment, TableConfig
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.datastream import RocksDBStateBackend


class FlinkJob:

    job_name = None
    tables = []
    flink_sql_connector_kafka_jar = "file:///home/ubuntu/flink-sql-connector-kafka_2.11-1.12.0.jar"
    checkpoints_path = "file:///opt/flink/checkpoints"
    

    def __init__(self, parallelism: int = 1,
                checkpoint_interval: Optional[int] = None,
                state_ttl: Optional[int] = None) -> None:
        # setting env
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(parallelism)
        if checkpoint_interval:
            env.set_state_backend(RocksDBStateBackend(
                self.checkpoints_path, enable_incremental_checkpointing=True))
            env.enable_checkpointing(checkpoint_interval*1000)
        t_config = TableConfig()
        if state_ttl:
            t_config.set_idle_state_retention_time(timedelta(seconds=state_ttl),
                                                   timedelta(seconds=state_ttl+300))
        table_env = StreamTableEnvironment.create(env, table_config=t_config)
        table_env.get_config().get_configuration().set_string(
            "pipeline.jars", self.flink_sql_connector_kafka_jar)
        
        # set up table
        for ddl in self.tables:
            table_env.execute_sql(ddl)

        self.env = env
        self.table_env = table_env

    def sink_to_kafka(self, topic: str, table: Table,
                      typeinfo: TypeInformation, pool_size: int = 1) -> None:
        builder = JsonRowSerializationSchema.builder()
        builder.with_type_info(typeinfo)
        stream = self.table_env.to_retract_stream(table, typeinfo)
        stream = stream.filter(lambda x: x[0])\
                       .map(lambda x: x[1], output_type=typeinfo)
        stream.add_sink(
            FlinkKafkaProducer(
                topic, builder.build(),
                producer_config={
                    'bootstrap.servers': self.kafka_addr},
                kafka_producer_pool_size=pool_size
            )
        )
    
    def process(self) -> None:
        raise NotImplementedError

    def run(self) -> None:
        self.process()
        self.env.execute(self.job_name)

