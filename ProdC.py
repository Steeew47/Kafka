from confluent_kafka import Producer
import time


p = Producer({'bootstrap.servers': 'localhost:9092',
                "queue.buffering.max.ms": 2}
                )
p.flush()


def delivery_report(err, msg,):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message offset: {} delivered to {} [{}]'.format(msg.offset(),msg.topic(), msg.partition()))

m_count = 0;

while m_count <= 10000:

    #p.poll(0)

    m_count += 1;
    p.produce('testTopic', 'Test1', callback=delivery_report)

p.produce('testTopic', 'Stop1')
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
