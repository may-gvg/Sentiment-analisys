import detectlanguage  # https://detectlanguage.com/
import sys
import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

detectlanguage.configuration.api_key = '6603423ca157fb36214dff1f46b03214'


running = True

def connect_kafka_producer():
    _producer = None
    conf = {'bootstrap.servers': "localhost:9092"}
    try:
        _producer = Producer(conf)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.produce(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def detect_lang():
    """Produce massages in detect topic if poem is in English language"""

    producer = connect_kafka_producer()
    consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'detection',
    'auto.offset.reset': 'earliest'
    })

    print('Running Consumer..')
    topic_name = 'poems'
    parsed_topic_name = 'detect'

    try:
        consumer.subscribe([topic_name])

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # msg_process(msg)
                
                poem_object = json.loads(msg.value())
                lang = detectlanguage.detect(poem_object['content'])
                if lang[0]['language'] == 'en':
                    publish_message(producer, parsed_topic_name, poem_object['title'], poem_object['content'])
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

    return 'detect_lang function ended'

if __name__ == '__main__':
    detect_lang()