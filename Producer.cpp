#include <cppkafka/cppkafka.h>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <string>

using namespace std;
using namespace cppkafka;

int main() {

    Configuration config = {
        { "metadata.broker.list", "127.0.0.1:9092"},
        {"queue.buffering.max.ms" , 1},
        {"linger.ms", 1}
    };

    Producer producer(config);
    int i = 0;

    while(i<10000){
      string message = to_string(i);
      producer.produce(MessageBuilder("testTopic").partition(0).payload(message));
      cout.flush();
      i++;
      cout << "Message sent"<< message << endl;
    }



    producer.flush();
}
