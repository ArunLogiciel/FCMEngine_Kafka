#include <iostream>
#include <string>
//#include "../Consumer/FeeModule/librdkafka/rdkafkacpp.h"
#include <librdkafka/rdkafkacpp.h>

int main() {
    std::string errstr;

    // 1. Create global config
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "localhost:9092", errstr);

    // 2. Create producer
    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    std::string topic = "EOD-Jobs";
    std::string message = "LoadDataOfDate 20260303";

    // 3. Produce message
    RdKafka::ErrorCode resp = producer->produce(
        topic,                              // topic name
        RdKafka::Topic::PARTITION_UA,        // let Kafka choose partition
        RdKafka::Producer::RK_MSG_COPY,      // copy payload
        const_cast<char*>(message.data()),  // payload
        message.size(),                     // payload size
        nullptr,                            // key
        0,                                  // key length
        0,                                  // timestamp
        nullptr                             // message opaque
    );


    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Produce failed: "
            << RdKafka::err2str(resp) << std::endl;
    }
    else {
        std::cout << "Message sent successfully!" << std::endl;
    }

    // 4. Wait for delivery
    producer->flush(5000);

    delete producer;
    delete conf;

    return 0;
}
