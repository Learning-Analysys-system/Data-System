import datetime
from minio import Minio
from kafka_utils.kafka import kafkautils
from minio_utils.minio import MinioClient
import os
import json
import io


def load_to_minio():
    consumer = kafkautils.create_consumer(os.getenv("BOOTSTRAPSERVERS"), topic='demo_java')
    minio_client = MinioClient()
    bucket_name = 'logsystem'

    records = consumer.poll(timeout_ms=5000)
    # print(consumer.assignment())
    print(consumer.partitions_for_topic('demo_java'))

    print("Bootstrap connected:", consumer.bootstrap_connected())
    print("Available topics:", consumer.assignment())

    if not records:
        print("No messages yet...")
        return "No messages yet..."
    else:
        print("haveee message")
        
    data = []
    for _, msgs in records.items():
        for msg in msgs:
            print(msg.value.decode('utf-8')[0])

            if msg.value.decode('utf-8')[0] == '{':
                data.append(json.loads(msg.value.decode('utf-8').replace('\'', '\"')))


    json_bytes = json.dumps(data).encode('utf-8')


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