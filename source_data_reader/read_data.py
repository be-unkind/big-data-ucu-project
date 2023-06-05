from kafka import KafkaProducer
import requests
import json
import time

def read_stream(producer, stream_url):
    response = requests.get(stream_url, stream=True)

    time.sleep(5)

    for line in response.iter_lines():

        if line:
            decoded_line = line.decode('utf8')

            if decoded_line.startswith('data:'):
                try:
                    json_data = json.loads(decoded_line[6:]) 
                    producer.send('source-data', json_data)
                    producer.flush()
                    
                except json.decoder.JSONDecodeError:
                    continue

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['project-kafka:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    stream_url = 'https://stream.wikimedia.org/v2/stream/page-create'

    read_stream(producer, stream_url)

