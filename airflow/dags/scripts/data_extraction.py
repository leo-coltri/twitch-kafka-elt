import requests
import json
import time
from datetime import date
from kafka import KafkaProducer


def create_kafka_producer():
    return KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

def make_request(endpoint, headers, params = None):
    response = requests.get(endpoint, headers=headers, params=params)
    return json.loads(response.content)

def extract_stream_data(endpoint, headers, ts_nodash=None):
    stream_data = []
    producer = create_kafka_producer()
    # Set the initial pagination cursor to None
    pagination_cursor = None
    i = 0
    end_time = time.time() + 10
    while True:

        # Set the query parameters and pagination cursor for the API request
        params = {
            "first": 100,
            "after": pagination_cursor
        }

        # Make a GET request to the Twitch API endpoint with the headers and query parameters
        response = make_request(endpoint, headers, params)
        stream_data += response["data"]
        
        producer.send("topic_54", json.dumps(stream_data).encode('utf-8'))
        
        # Check if there are more pages of streamer data to retrieve
        if "pagination" in response and "cursor" in response["pagination"]:
            pagination_cursor = response["pagination"]["cursor"]
        else:
            break
    
        if i == 1:
            break
        else: 
            i = i + 1