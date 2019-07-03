import time
from kafka import KafkaProducer
from json import dumps

boostrap_server = ['localhost:9092']
topicName = 'testTopic'



def Producer():

    producer = KafkaProducer(bootstrap_servers=boostrap_server, value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

    k = 0
    while k<3:
        start = time.time()
        producer.send(topicName, start)
        print('Message sent')
        time.sleep(0.5)



    producer.close()


def main():
    Producer()


if __name__ == "__main__":
    main()
