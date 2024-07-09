from pyspark.sql import SparkSession
import logging
def create_spark_session():
    print("creaing")
    try:
        session = SparkSession.builder.appName("kafka-stream") \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .master("spark://192.168.0.138:7077") \
        .getOrCreate() 
        session.sparkContext.setLogLevel("ERROR")
        logging.info("connected succesfully")
        print("connnected")
        return session
    except Exception as e:
        logging.error(f'failed to connect err : {e}')
        print("error")

def kafka_conn(conn):
    try:
        df = conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'record-cassandra-leaves-avro') \
            .option('startingOffsets', 'earliest') \
            .load()
        print(df)
        df_str = df.selectExpr("CAST(value as string)")
        df_str.printSchema()
    except Exception as e:
        raise e 


# if __name__ == '__main___':
print("invoking")
conn = create_spark_session()
print(conn)
if conn is not None:
    print("here is the schema")
    kafka_conn(conn)