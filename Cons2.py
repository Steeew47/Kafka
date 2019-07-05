import time
from kafka import KafkaConsumer, KafkaProducer
import matplotlib.pyplot as plt
from matplotlib.pyplot import *

boostrap_server = ['localhost:9092']
topicName = 'testTopic'
consumer_timeout_ms = 1000
plot_time = []


def Consumer():


    consumer = KafkaConsumer(bootstrap_servers=boostrap_server,auto_offset_reset='latest',consumer_timeout_ms=consumer_timeout_ms)
    consumer.subscribe([topicName])
    print('Waiting for message...')
    m_number = 0

    fig = plt.gcf()
    fig.show()
    fig.canvas.draw()

    for message in consumer:
        stop = time.time()
        start = message.value
        timestamp = int(message.timestamp)
        print('No. {0}'.format(m_number))
        print('Message sent time    : {0}'.format(start))
        print('Message recieve time : {0:.6f}'.format(stop))
        m_time = (float(stop) - float(start))*1000
        plot_time.append(m_time)
        print('Message latency      : {0} [ms]'.format(m_time))
        print('\n')
        plt.plot(plot_time[:],color='blue')
        plt.xlabel('No. Message')
        plt.ylabel('Latency [ms]')
        fig.canvas.draw()
        m_number +=1

    stop = time.time()


    consumer.close()

def main():
    Consumer()


if __name__ == "__main__":
    main()
