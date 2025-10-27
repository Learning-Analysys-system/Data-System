import json
from spark_utils.spark import SparkUtils, col, to_timestamp

def order_xAPI_data_from_kafka(records):
    """
        records: records object from kafka
    """

    ordered_data = []
    unordered_data = []
    for _, msgs in records.items():
        for msg in msgs:
            print(msg.value.decode('utf-8')[0])

            if msg.value.decode('utf-8')[0] == '{':
                datajson = json.loads(msg.value.decode('utf-8').replace('\'', '\"'))
                data = {
                    "time_stamp": datajson["timestamp"],
                    "data": json.dumps(datajson)
                }
                unordered_data.append(data)
    
    with SparkUtils() as spark:
        df = spark.createDataFrame(unordered_data)
        df_cast_timestamp = df.withColumn("time_parsed", to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        df_ordered = df_cast_timestamp.orderBy(col('time_parsed'))

    for row in df_ordered.collect():
        print(type(row["data"]))
        data = json.loads(row["data"])
        ordered_data.append(data)

    return ordered_data