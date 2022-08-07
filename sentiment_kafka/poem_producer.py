from confluent_kafka import Producer
import sqlite3
import json


def database_number_of_records():
    try:
        sqliteConnection = sqlite3.connect('db.sqlite3')
        cursor = sqliteConnection.cursor()

        cursor.execute("SELECT COUNT(*) FROM poetry_app_poemmodel;")
        number_of_records = cursor.fetchall()

        cursor.close()

        return number_of_records
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()


def get_record_by_id(id):
    try:

        sqliteConnection = sqlite3.connect('db.sqlite3')
        cursor = sqliteConnection.cursor()
        cursor.execute(f"SELECT * FROM poetry_app_poemmodel WHERE id={id};")
        id_record = cursor.fetchall()
        cursor.close()

        return id_record
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()


def create_json(id_number):
    poem_dictionary = {"title": get_record_by_id(id_number)[0][1],
                       "content": get_record_by_id(id_number)[0][2],
                       "url": get_record_by_id(id_number)[0][3],
                       "poet_name": get_record_by_id(id_number)[0][4],
                       "poet_url": get_record_by_id(id_number)[0][5]
                       }
    poem_json = json.dumps(poem_dictionary, indent=4)

    return poem_json


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


if __name__ == '__main__':

    kafka_producer = connect_kafka_producer()
    records_number = database_number_of_records()[0][0]
    topic_name = 'poems'

    while records_number > 0:

        json_record = create_json(records_number)
        publish_message(kafka_producer, topic_name, 'title', json_record)
        records_number -= 1