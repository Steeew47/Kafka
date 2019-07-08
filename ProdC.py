from confluent_kafka import Producer
import time


p = Producer({'bootstrap.servers': 'localhost:9092',
                "queue.buffering.max.ms": 1})

# queue.buffering.max.ms": 2

def delivery_report(err, msg,):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message offset: {} delivered to {} [{}]'.format(msg.offset(),msg.topic(), msg.partition()))

m_count = 0;
called = True

while m_count <= 10000:

    #p.poll(0.01)

    m_count += 1;
    #if m_count%30 == 0:
    #    time.sleep(0.01)

    if called is False:
        start = time.time()
        called = True

    p.produce('testTopic', 'Test1', callback=delivery_report
    )

p.produce('testTopic', 'Stop1')
p.flush()
