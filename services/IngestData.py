import os
import requests
from requests.auth import HTTPBasicAuth
from kafka_utils.kafka import kafkautils

def get_statements():

    API_ENDPOINT = os.getenv("API_ENDPOINT_SCORM")           # Trả về None nếu biến không tồn tại
    API_KEY = os.environ.get("API_KEY_SCORM") 
    API_SECRET = os.environ.get("API_SECRET_SCORM") 

    producer = kafkautils.create_producer(bootstrapServers=os.getenv("BOOTSTRAPSERVERS"))
    headers = {
        "X-Experience-API-Version": "1.0.3"
    }
    params = {
        "limit": 100
    }
    try:
        response = requests.get(API_ENDPOINT, headers=headers, params=params, auth=HTTPBasicAuth(API_KEY, API_SECRET))
        print(response.json())
        
        if response.status_code == 200:
            for response in response.json()['statements']:
                producer.send(topic = 'demo_java' , value = str(response).encode('utf-8'))
            
            producer.close()
        else:
            print(f"Request failed with status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise str(e)

