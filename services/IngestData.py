import os
import requests
from requests.auth import HTTPBasicAuth
from kafka_utils.kafka import kafkautils

def get_statements():

    API_ENDPOINT = os.getenv("API_ENDPOINT_SCORM")           # Trả về None nếu biến không tồn tại
    API_KEY = os.environ.get("API_KEY_SCORM") 
    API_SECRET = os.environ.get("API_SECRET_SCORM") 

    bootstrap_servers = os.getenv("BOOTSTRAPSERVERS")
    if not bootstrap_servers:
        raise ValueError("BOOTSTRAPSERVERS environment variable is not set. Please set it in .env file (e.g., localhost:29092)")
    
    producer = None
    try:
        try:
            producer = kafkautils.create_producer(bootstrapServers=bootstrap_servers)
        except Exception as e:
            raise Exception(f"Failed to create Kafka producer: {str(e)}. Please ensure Kafka is running at {bootstrap_servers}")
        
        headers = {
            "X-Experience-API-Version": "1.0.3"
        }
        params = {
            "limit": 100
        }
        
        response = requests.get(API_ENDPOINT, headers=headers, params=params, auth=HTTPBasicAuth(API_KEY, API_SECRET))
        print(response.json())
        
        if response.status_code == 200:
            for stmt in response.json()['statements']:
                producer.send(topic='demo_java', value=str(stmt).encode('utf-8'))
        else:
            raise Exception(f"Request failed with status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise Exception(str(e))
    finally:
        if producer:
            producer.close()

