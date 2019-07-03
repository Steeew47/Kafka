import time
from kafka import KafkaProducer
from json import dumps

boostrap_server = ['localhost:9092']
topicName = 'testTopic'
batch_size = 0


def Producer():

    producer = KafkaProducer(bootstrap_servers=boostrap_server, value_serializer=lambda x:
                         dumps(x).encode('utf-8'), batch_size = batch_size)


    while True:
        start = time.time()
        producer.send(topicName, start)
        print('Message sent')
        time.sleep(0.5)

    producer.close()


def main():
    Producer()


if __name__ == "__main__":
    main()
