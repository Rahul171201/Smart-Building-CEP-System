import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
from pyflink.table.window import Tumble

 def register_transactions_source(st_env):
    st_env.connect(Kafka()
                   .version("universal")
                   .topic("test-topic")
                   .start_from_latest()
                   .property("zookeeper.connect", "host.docker.internal:2181")
                   .property("bootstrap.servers", "host.docker.internal:19092")) \
        .with_format(Json()
        .fail_on_missing_field(True)
        .schema(DataTypes.ROW([
        DataTypes.FIELD("Temperature", DataTypes.DOUBLE()),
        DataTypes.FIELD("Humidity", DataTypes.DOUBLE()),
        DataTypes.FIELD("Light", DataTypes.DOUBLE()),
        DataTypes.FIELD("CO2", DataTypes.DOUBLE()),
        DataTypes.FIELD("HumidityRatio", DataTypes.DOUBLE()),
        DataTypes.FIELD("Occupancy", DataTypes.INTEGER()),
        DataTypes.FIELD("date", DataTypes.TIMESTAMP())]))) \
        .with_schema(Schema()
        .field("Temperature", DataTypes.DOUBLE())
        .field("Humidity", DataTypes.DOUBLE())
        .field("Light", DataTypes.DOUBLE())
        .field("CO2", DataTypes.DOUBLE())
        .field("HumidityRatio", DataTypes.DOUBLE())
        .field("Occupancy", DataTypes.DOUBLE())
        .field("date", DataTypes.TIMESTAMP())
        .rowtime(
        Rowtime()
            .timestamps_from_field("date")
            .watermarks_periodic_bounded(60000))) \
        .in_append_mode() \
        .register_table_source("source")
        
def register_transactions_sink_into_csv(st_env):
    result_file = "/opt/examples/data/output/output_file.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env.register_table_sink("sink_into_csv",
                               CsvTableSink(["Temperature",
                                             "Humidity",
                                             "Light",
                                             "CO2",
                                             "HumidityRatio",
                                             "Occupancy",
                                             "last_time"],
                                            [DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.DOUBLE(),
                                             DataTypes.TIMESTAMP()],
                                            result_file))
                                            
def transactions_job():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    register_transactions_source(st_env)
    register_transactions_sink_into_csv(st_env)

    st_env.from_path("source") \
        .window(Tumble.over("10.hours").on("date").alias("w")) \
        .group_by("Temperature, w") \
        .select("""Temperature as Temperature, 
                   Humidity as Humidity,
                   Light as Light, 
                   CO2 as CO2,
                   HumidityRatio as HumidityRatio,
                   Occupancy as Occupancy,
                   w.end as last_date
                   """) \
        .filter("Humidity < 27") \ 
        .filter("Light>=3") \
        .filter("CO2>550.00") \ .filter("CO2 < 600.00") \
        .insert_into("sink_into_csv")

    st_env.execute("app")


if __name__ == '__main__':
    usage_job()
