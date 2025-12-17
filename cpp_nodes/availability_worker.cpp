#include "worker_base.hpp"
#include <random>

class AvailabilityWorker : public WorkerBase {
private:
    std::mt19937 m_gen;
    std::uniform_int_distribution<int> m_seats_dist;

public:
    AvailabilityWorker(const std::string& endpoint): WorkerBase(endpoint, "availability_worker"), m_gen(std::random_device{}()), m_seats_dist(0, 200) {}

    std::string process_request(const std::string& req) override {
        auto parts = split(req, '|');
        
        // Protocol Validation
        if (parts.size() < 5 || parts[0] != "AVAILABILITY") {
            return "{\"type\":\"ERROR\",\"message\":\"Invalid availability request format\"}";
        }

        const std::string &request_id = parts[1];
        const std::string &origin = parts[2];
        const std::string &destination = parts[3];
        const std::string &date = parts[4];

        // Simulate work
        std::this_thread::sleep_for(std::chrono::milliseconds(100 + (m_seats_dist(m_gen) % 200)));

        // Business Logic
        int seats = m_seats_dist(m_gen);
        bool available = seats > 0;

        // Construct JSON
        std::string resp = "{";
        resp += "\"type\":\"AVAILABILITY\",";
        resp += "\"request_id\":\"" + request_id + "\",";
        resp += "\"origin\":\"" + origin + "\",";
        resp += "\"destination\":\"" + destination + "\",";
        resp += "\"date\":\"" + date + "\",";
        resp += "\"available\":" + std::string(available ? "true" : "false") + ",";
        resp += "\"seats\":" + std::to_string(seats);
        resp += "}";

        return resp;
    }
};

int main(int argc, char **argv) {
    std::string endpoint = "tcp://*:5556";
    if (argc > 1) {
        endpoint = argv[1];
    }
    
    AvailabilityWorker worker(endpoint);
    worker.run();
    
    return 0;
}