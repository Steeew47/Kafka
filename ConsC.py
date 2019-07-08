from confluent_kafka import Consumer, KafkaError
import time
import matplotlib.pyplot as plt


latency = []

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'testTopic',
    'auto.offset.reset': 'latest',
    'fetch.wait.max.ms' : 1,
    'metadata.request.timeout.ms' : 1000})

#fetch.wait.max.ms : 5

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

c.subscribe(['testTopic'],on_assign=print_assignment)

m_number = 0
called = True


while True:
    msg = c.poll(30)


    if msg is None:
        break;
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    if called is True:
        start = time.time()
        called = False

    m_time = int(round(time.time() * 1000)) - msg.timestamp()[1]
    #print(m_time)
    latency.append(m_time)
    m_number +=1






print("Average latency : {0}".format(sum(latency)/m_number))
plt.plot(latency)
plt.ylabel('Letency : [ms]')
plt.xlabel('Number of messages')
plt.show()


c.close()
