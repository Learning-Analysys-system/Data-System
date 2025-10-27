from fastapi import FastAPI
from fastapi.responses import JSONResponse
from services.IngestData import get_statements
from services.LoadDataLake import load_to_minio
from services.LoadDatawarehouse import ETL_To_DataWarehouse
from dotenv import load_dotenv
import datetime
load_dotenv()

app = FastAPI()

@app.get("/ingest_log_xAPI")
async def IngestData():
    try:
        get_statements()
        return JSONResponse(content={"message": "Data Ingested Successfully."}, status_code=200)
    except Exception as e:
        return JSONResponse({"message": f"Data Ingestion Failed: {str(e)}"}, status_code=500)


@app.get("/load_to_datalake")
async def load_to_datalake():
    try:
        message = load_to_minio()
        return JSONResponse(content={"message": message}, status_code=200)

    except Exception as e:
        print(str(e))
        return JSONResponse({"message": f"Load fail: {str(e)}"}, status_code=500)


@app.get("/load_to_datawarehouse")
async def load_to_datawarehouse(bucket_name:str, date_to_extract:str):
    elt_to_datawarehouse = ETL_To_DataWarehouse(bucket_name)

    try:
        objects_name = elt_to_datawarehouse.extractData(date_to_extract)
        message = elt_to_datawarehouse.loadData(objects_name)
        return JSONResponse(content={"message": message}, status_code=200)

    except Exception as e:
        print(str(e))

        return JSONResponse({"message": f"Load fail: {str(e)}"}, status_code=500)

