#include "worker_base.hpp"
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <vector>
#include <cmath>

class PricingWorker : public WorkerBase {
private:
    std::mt19937 m_gen;

    // Helper logic to calculate price based on distance, type, and date
    double calculatePrice(const std::string &origin, const std::string &destination, const std::string &date, const std::string &type) {
        double basePrice = 100.0;
        
        // Distance factor (simplified: difference in first letter ASCII value * 50)
        int distance = std::abs(static_cast<int>(origin[0]) - static_cast<int>(destination[0])) * 50;
        
        // Type factor
        double typeFactor = (type == "flight") ? 2.5 : 1.8; // hotels cheaper than flights
        
        // Date factor (weekend premium check)
        double dateFactor = 1.0;
        if (date.find("2025-12-06") != std::string::npos || 
            date.find("2025-12-07") != std::string::npos) {
            dateFactor = 1.3; // weekend premium
        }
        
        // Random market factor (fluctuation between 0.8x and 1.4x)
        std::uniform_real_distribution<double> marketDist(0.8, 1.4);
        double marketFactor = marketDist(m_gen);
        
        return basePrice + distance * typeFactor * dateFactor * marketFactor;
    }

public:
    PricingWorker(const std::string& endpoint): WorkerBase(endpoint, "pricing_worker"), m_gen(std::random_device{}()) {}

    std::string process_request(const std::string& req) override {
        // Use the split helper from WorkerBase
        auto parts = split(req, '|');

        // Protocol Validation
        if (parts.size() < 6 || parts[0] != "PRICING") {
             return "{"
                    "\"type\":\"ERROR\","
                    "\"message\":\"Invalid pricing request format\""
                    "}";
        }

        const std::string &request_id = parts[1];
        const std::string &origin = parts[2];
        const std::string &destination = parts[3];
        const std::string &date = parts[4];
        const std::string &type = parts[5];

        // Simulate computation time (CPU bound simulation)
        // using rand() for the sleep duration to keep it simple as per original logic
        std::this_thread::sleep_for(std::chrono::milliseconds(150 + (rand() % 300)));

        // Perform the calculation
        double price = calculatePrice(origin, destination, date, type);

        // Construct JSON Response
        std::string resp = "{";
        resp += "\"type\":\"PRICING\",";
        resp += "\"request_id\":\"" + request_id + "\",";
        resp += "\"origin\":\"" + origin + "\",";
        resp += "\"destination\":\"" + destination + "\",";
        resp += "\"date\":\"" + date + "\",";
        resp += "\"travel_type\":\"" + type + "\",";
        resp += "\"price\":" + std::to_string(price) + ",";
        resp += "\"currency\":\"USD\"";
        resp += "}";

        return resp;
    }
};

int main(int argc, char **argv) {
    // Endpoint can be overridden via CLI, default to TCP 5557.
    std::string endpoint = "tcp://*:5557";
    if (argc > 1) {
        endpoint = argv[1];
    }

    // Instantiate and run
    PricingWorker worker(endpoint);
    worker.run();

    return 0;
}