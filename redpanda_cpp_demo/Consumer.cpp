#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

int main() {
    std::string errstr;

    // 1️⃣ Create global config
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "localhost:9092", errstr);  // Redpanda/Kafka broker
    conf->set("group.id", "demo-group", errstr);              // Consumer group
    conf->set("auto.offset.reset", "earliest", errstr);       // Start at beginning if no offset

    // 2️⃣ Create consumer
    RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return 1;
    }

    // 3️⃣ Subscribe to the topic
    std::vector<std::string> topics = { "demo-topic" };
    RdKafka::ErrorCode resp = consumer->subscribe(topics);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe to topic: " << RdKafka::err2str(resp) << std::endl;
        return 1;
    }

    std::cout << "Consumer started. Waiting for messages..." << std::endl;

    // 4️⃣ Poll loop
    while (true) {
        RdKafka::Message* msg = consumer->consume(1000); // Timeout = 1000ms

        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            std::cout << "[Partition " << msg->partition()
                << " | Offset " << msg->offset()
                << "] Message: "
                << std::string(static_cast<const char*>(msg->payload()), msg->len())
                << std::endl;
            break;

        case RdKafka::ERR__TIMED_OUT:
            // No message received in this poll
            break;

        default:
            std::cerr << "Consumer error: " << msg->errstr() << std::endl;
            break;
        }

        delete msg;
    }

    // 5️⃣ Cleanup (won’t actually reach here in this infinite loop)
    consumer->close();
    delete consumer;
    delete conf;

    return 0;
}
