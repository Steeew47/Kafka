from kafka import KafkaProducer, KafkaConsumer
import time

#Message
msg_count = 10000
msg_size = 100 #bytes
msg_payload = ('adamadam' * 20).encode()[:msg_size]
payload_size = msg_size * msg_count

#Configure settings
bootstrap_servers = 'localhost:9092'
topic = 'testTopic'
auto_offset_reset = 'earliest'

#Timers
producer_time = {}
consumer_time = {}

def time_In(timing, n_messages=msg_count, msg_size=msg_size):
    print("Uploaded {0} messsages in {1:.2f} seconds to the partition".format(n_messages, timing))
    print("Upload speed: {0:.2f} MB/s".format((msg_size * n_messages) * 1e-6 / timing ))


def time_Out(timing, n_messages=10000, msg_size=100):
    print("Downloaded {0} messsages in {1:.2f} seconds from the partition".format(n_messages, timing))
    print("Download speed: {0:.2f} MB/s".format((msg_size * n_messages) * 1e-6 / timing ))



def Producer():
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer_start = time.time()

    for i in range(msg_count):
        producer.send(topic, msg_payload)

    producer.flush()
    return time.time() - producer_start

def Consumer():

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,auto_offset_reset = auto_offset_reset,)
    msg_consumed_count = 0

    consumer_start = time.time()
    consumer.subscribe([topic])
    for msg in consumer:
        msg_consumed_count += 1

        if msg_consumed_count >= msg_count:
            break

    consumer_timing = time.time() - consumer_start
    consumer.close()
    print consumer_timing
    return consumer_timing

print("Message payload : "+msg_payload)

producer_time['Producer'] = Producer()
time_In(producer_time['Producer'])

_ = Producer()
consumer_time['Consumer'] = Consumer()
time_Out(consumer_time['Consumer'])
