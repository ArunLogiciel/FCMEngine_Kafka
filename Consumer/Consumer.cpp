#include <iostream>
#include <string>
#include <sstream>
//#include "../Consumer/FeeModule/librdkafka/rdkafkacpp.h"
#include <librdkafka/rdkafkacpp.h>
#include "../Consumer/FeeModule/businessObjects.h"
#include "../Consumer/FeeModule/FCMEngine.h"


void processCommand(const std::string& message) {
    std::cout << "Processing: " << message << std::endl;

    std::istringstream iss(message);
    std::string command;
    iss >> command;

    if (command == "LoadDataOfDate") {
        std::string date;
        iss >> date;

        if (date.empty()) {
            std::cerr << "Error: Missing date!" << std::endl;
            return;
        }

        // 🔥 Simulate your FCMEngine logic
        std::cout << "[ENGINE] Loading data for date: " << date << std::endl;

        // TODO:
        Adjustment adj;
        adj.date = date;
        adj.adjustmentType = EN_Adjustment_Everything;
        adj.workFlowType = EN_WorkFlowType_Ingestion;

        std::cout << "Asjustment Details: " << adj.date << std::endl << adj.adjustmentType << std::endl << adj.workFlowType << std::endl;
        FCM_Engine engine("FCMConfiguration.ini", 1, date);
        // readTradeFiles(date)
        // calculateFees()
        // writeToDB()

    }
    else {
        std::cerr << "Unknown command: " << command << std::endl;
    }
}
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
    std::vector<std::string> topics = { "EOD-Jobs" };
    RdKafka::ErrorCode resp = consumer->subscribe(topics);
    if (resp != RdKafka::ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe to topic: " << RdKafka::err2str(resp) << std::endl;
        return 1;
    }

    std::cout << "Consumer started. Waiting for messages..." << std::endl;

    // 4️⃣ Poll loop
    while (true) {
        RdKafka::Message* msg = consumer->consume(1000); // Timeout = 1000ms

        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            std::string payload(static_cast<const char*>(msg->payload()), msg->len());
            processCommand(payload);
        }
        else if (msg->err() == RdKafka::ERR__TIMED_OUT) {
            // No message received in this poll
        }
        else {
            std::cerr << "Consumer error: " << msg->errstr() << std::endl;
        }

        delete msg;
    }

    // 5️⃣ Cleanup (won't actually reach here in this infinite loop)
    consumer->close();
    delete consumer;
    delete conf;

    return 0;
}