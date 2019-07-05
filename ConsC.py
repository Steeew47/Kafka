from confluent_kafka import Consumer, KafkaError
import time
import matplotlib.pyplot as plt


latency = []

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'testTopic',
    'auto.offset.reset': 'latest',
    'queue.buffering.max.ms': 2,
    'fetch.wait.max.ms' : 2,
})

c.subscribe(['testTopic'])

m_number = 0


while True:
    msg = c.poll(1.0)

    if msg is None:
        print("Waiting for messages...")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    stop = int(round(time.time() * 1000))
    m_time = stop - msg.timestamp()[1]
    #print(m_time)
    latency.append(m_time)
    m_number +=1

    if(msg.value() == b'Stop1'):
        break




print("Average latency : {0}".format(sum(latency)/m_number))
plt.plot(latency)
plt.show()


c.close()
