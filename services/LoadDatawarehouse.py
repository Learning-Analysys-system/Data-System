from minio_utils.minio import MinioClient
from msqlserver_utils.msqlserver import MSQLServer, safe_get
import datetime


class ETL_To_DataWarehouse:
    def __init__(self, bucket_name):
        self.minioClient = MinioClient()
        self.bucket_name = bucket_name
        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Check if bucket exists, create if it doesn't"""
        try:
            if not self.minioClient.check_bucket_exists(self.bucket_name):
                self.minioClient.create_bucket(self.bucket_name)
                print(f"Created bucket: {self.bucket_name}")
            else:
                print(f"Bucket {self.bucket_name} already exists")
        except Exception as e:
            print(f"Error checking/creating bucket {self.bucket_name}: {str(e)}")
            raise

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
            for obj_name in objects_name:
                data = self.minioClient.get_object(
                    bucket_name=self.bucket_name,
                    object_name = obj_name
                )
                print(obj_name)
                for stmt in data.json():
                    try:
                        conn.insert_dim_actor(stmt.get("actor", {}))
                        conn.insert_dim_verb(stmt.get("verb", {}))
                        conn.insert_activity_detail(stmt.get("object", {}))

                        # context + bridge
                        context = stmt.get("context", {})
                        context_id = None
                        if context:
                            context_id = conn.insert_dim_context(context)
                            conn.insert_bridge_context_activity(context_id, safe_get(context, "contextActivities"))

                        # fact_statement
                        conn.insert_fact_statement(stmt, context_id)

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