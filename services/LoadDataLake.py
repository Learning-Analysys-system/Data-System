import datetime
from minio import Minio
from kafka_utils.kafka import kafkautils
from minio_utils.minio import MinioClient
from spark_utils.spark import SparkUtils
from services.ArrangeData import order_xAPI_data_from_kafka
import os
import json
import io


def load_to_minio():
    bootstrap_servers = os.getenv("BOOTSTRAPSERVERS")
    if not bootstrap_servers:
        raise ValueError("BOOTSTRAPSERVERS environment variable is not set. Please set it in .env file (e.g., localhost:29092)")
    
    consumer = None
    try:
        try:
            consumer = kafkautils.create_consumer(bootstrap_servers, topic='demo_java')
        except Exception as e:
            raise Exception(f"Failed to create Kafka consumer: {str(e)}. Please ensure Kafka is running at {bootstrap_servers}")
        
        minio_client = MinioClient()
        bucket_name = 'logsystem'

        try:
            records = consumer.poll(timeout_ms=5000)
            # print(consumer.assignment())
            print(consumer.partitions_for_topic('demo_java'))

            print("Bootstrap connected:", consumer.bootstrap_connected())
            print("Available topics:", consumer.assignment())
        except Exception as e:
            raise Exception(f"Failed to poll Kafka messages: {str(e)}. Please ensure Kafka broker is running and accessible.")

        if not records:
            print("No messages yet...")
            return "No messages yet..."
        else:
            print("haveee message")
            
        # oredered_data = []
        # unordered_datas = []
        # for _, msgs in records.items():
        #     for msg in msgs:
        #         print(msg.value.decode('utf-8')[0])

        #         if msg.value.decode('utf-8')[0] == '{':
        #             datajson = json.loads(msg.value.decode('utf-8').replace('\'', '\"'))
        #             data = {
        #                 "time_stamp": datajson["timestamp"],
        #                 "data": datajson
        #             }
        #             unordered_datas.append(data)

        oredered_data = order_xAPI_data_from_kafka(records)

        json_bytes = json.dumps(oredered_data).encode('utf-8')

        # Ensure bucket exists
        found = minio_client.check_bucket_exists(bucket_name=bucket_name)
        if not found:
            minio_client.create_bucket(bucket_name=bucket_name)
            print("Created bucket", bucket_name)
        else:
            print("Bucket", bucket_name, "already exists")

        clean_date = str(datetime.datetime.now().date()).replace('-', '/')
        clean_time = str(datetime.datetime.now().time()).split('.')[0]

        minio_client.put_object(
            bucket_name= bucket_name,
            destination_file=f'{clean_date}/{clean_time}_log_xAPI.json',
            data=io.BytesIO(json_bytes),
            length_data=len(json_bytes)
        )
        
        return "Data loaded to MinIO successfully."
    except Exception as e:
        raise Exception(f"Error processing data: {str(e)}")
    finally:
        if consumer:
            consumer.close()