import json
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

def read_stream(stream_url, producer):
    event_source = EventSource(stream_url)

    try:
        for event in event_source:
            if event.event == 'message' and event.data:
                message = event.data.replace('"', "'")
                producer.send('source-data', message)
    except KeyboardInterrupt:
        producer.close()


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['project-kafka:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    stream_url = 'https://stream.wikimedia.org/v2/stream/page-create'

    read_stream(stream_url, producer)