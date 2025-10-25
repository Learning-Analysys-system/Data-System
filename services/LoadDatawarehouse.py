from minio_utils.minio import MinioClient
from msqlserver_utils.msqlserver import (
    MSQLServer,
    insert_dim_actor,
    insert_dim_verb,
    insert_activity_detail,
    insert_dim_context,
    insert_bridge_context_activity,
    insert_fact_statement,
    safe_get
)
import datetime


class ETL_To_DataWarehouse:
    def __init__(self, bucket_name):
        self.minioClient = MinioClient()
        self.bucket_name = bucket_name

    def extractData(self, date_to_extract: str, range_time_to_extract = None):
        """
            date_to_extract: Date which want to extract data
            range_time_to_extract: Range time in Date want to extract
        """
        # clean_date = str(date_to_extract).split(' ')[0].replace('-', '/')
        # print(date_to_extract, self.bucket_name)
        object_names = self.minioClient.get_objects_name(self.bucket_name, prefix=date_to_extract)
        return object_names

    def loadData(self, objects_name):
        """
            object_names: list object to load to datawarehouse
        """
        with MSQLServer() as conn:
            cursor = conn.cursor()
            
            for obj_name in objects_name:
                data = self.minioClient.get_object(
                    bucket_name=self.bucket_name,
                    object_name = obj_name
                )
                print(obj_name)
                for stmt in data.json():
                    try:
                        insert_dim_actor(cursor, stmt.get("actor", {}))
                        insert_dim_verb(cursor, stmt.get("verb", {}))
                        insert_activity_detail(cursor, stmt.get("object", {}))

                        # context + bridge
                        context = stmt.get("context", {})
                        context_id = None
                        if context:
                            context_id = insert_dim_context(cursor, context)
                            insert_bridge_context_activity(cursor, context_id, safe_get(context, "contextActivities"))

                        # fact_statement
                        insert_fact_statement(cursor, stmt, context_id)

                        conn.commit()
                        # print(f"✅ Inserted statement {stmt.get('id')}")
                        
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ Error inserting statement {stmt.get('id')}: {e}")
                        return(f"❌ Error inserting statement {stmt.get('id')}: {e}")
                        
        return "Load data to datawarehouse successfully"
    
if __name__ == '__main__':
    etl = ETL_To_DataWarehouse('logsystem')
    date_to_extract = '2025/10/24'
    objects_name = etl.extractData(date_to_extract)
    print(objects_name)
    message = etl.loadData(objects_name)
    print(message)