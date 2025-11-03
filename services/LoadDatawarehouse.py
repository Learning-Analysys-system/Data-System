from minio_utils.minio import MinioClient
from msqlserver_utils.msqlserver import (
    MSQLServer
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
        with MSQLServer() as msql_server:
            
            for obj_name in objects_name:
                data = self.minioClient.get_object(
                    bucket_name=self.bucket_name,
                    object_name = obj_name
                )
                print(obj_name)
                for stmt in data.json():
                    try:
                        msql_server.insert_dim_actor(stmt.get("actor", {}))
                        msql_server.insert_dim_verb(stmt.get("verb", {}))
                        msql_server.insert_activity_detail(stmt.get("object", {}))

                        # context + bridge
                        context = stmt.get("context", {})
                        context_id = None
                        if context:
                            context_id = msql_server.insert_dim_context(context)
                            msql_server.insert_bridge_context_activity(context_id, msql_server._safe_get(context, "contextActivities"))

                        # fact_statement
                        msql_server.insert_fact_statement(stmt, context_id)
                        msql_server.conn.commit()
                        # print(f"✅ Inserted statement {stmt.get('id')}")
                        
                    except Exception as e:
                        msql_server.conn.rollback()
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