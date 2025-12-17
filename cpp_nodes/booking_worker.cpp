#include "worker_base.hpp"
#include <unordered_map>
#include <mutex>
#include <random>
#include <thread>
#include <chrono>
#include <iostream>

// Internal helper class to manage state
class BookingDatabase {
private:
    std::unordered_map<std::string, std::string> bookings; // confirmation_code -> booking_details
    std::mutex db_mutex;
    std::random_device rd;
    std::mt19937 gen;

public:
    BookingDatabase() : gen(rd()) {}

    std::string generateConfirmationCode() {
        std::uniform_int_distribution<> dis(0, 35);
        std::string code;
        const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        
        for (int i = 0; i < 6; ++i) {
            code += chars[dis(gen)];
        }
        return code;
    }

    bool processBooking(
            const std::string &request_id, const std::string &customer_id,
            const std::string &origin, const std::string &destination,
            const std::string &date, const std::string &type,
            int passengers, std::string &confirmation_code, std::string &status)
    {
        std::lock_guard<std::mutex> lock(db_mutex);
        
        // Simulate booking validation
        std::uniform_real_distribution<> success_rate(0.0, 1.0);
        bool booking_success = success_rate(gen) > 0.05; // 95% success rate
        
        if (booking_success) {
            confirmation_code = generateConfirmationCode();
            std::string booking_details = request_id + "|" + customer_id + "|" + 
                                        origin + "|" + destination + "|" + date + "|" + 
                                        type + "|" + std::to_string(passengers);
            bookings[confirmation_code] = booking_details;
            status = "confirmed";
            return true;
        } else {
            confirmation_code = "";
            status = "failed";
            return false;
        }
    }
};

class BookingWorker : public WorkerBase {
private:
    BookingDatabase db;

public:
    // Pass the endpoint and a friendly name to the Base Constructor
    BookingWorker(const std::string& endpoint): WorkerBase(endpoint, "booking_worker") {}

    // Implement the pure virtual function from WorkerBase
    std::string process_request(const std::string& req) override {
        // Use the split() helper from WorkerBase
        auto parts = split(req, '|');

        // Protocol Validation
        if (parts.size() < 8 || parts[0] != "BOOKING") {
             return "{"
                    "\"type\":\"ERROR\","
                    "\"message\":\"Invalid booking request format\""
                    "}";
        }

        const std::string &request_id = parts[1];
        const std::string &customer_id = parts[2];
        const std::string &origin = parts[3];
        const std::string &destination = parts[4];
        const std::string &date = parts[5];
        const std::string &type = parts[6];
        int passengers = std::stoi(parts[7]);

        // Simulate booking processing time (random delay)
        // Note: We use a temporary random device here for the delay duration
        // to avoid locking the DB's generator if we don't have to.
        std::this_thread::sleep_for(std::chrono::milliseconds(300 + (rand() % 500)));

        std::string confirmation_code;
        std::string status;
        
        // Perform the stateful operation
        bool success = db.processBooking(request_id, customer_id, origin, destination, 
                                       date, type, passengers, confirmation_code, status);

        // Construct JSON Response
        std::string resp = "{";
        resp += "\"type\":\"BOOKING\",";
        resp += "\"request_id\":\"" + request_id + "\",";
        resp += "\"customer_id\":\"" + customer_id + "\",";
        resp += "\"origin\":\"" + origin + "\",";
        resp += "\"destination\":\"" + destination + "\",";
        resp += "\"date\":\"" + date + "\",";
        resp += "\"travel_type\":\"" + type + "\",";
        resp += "\"passengers\":" + std::to_string(passengers) + ",";
        resp += "\"status\":\"" + status + "\"";
        
        if (success) {
            resp += ",\"confirmation_code\":\"" + confirmation_code + "\"";
        }
        
        resp += "}";

        return resp;
    }
};

int main(int argc, char **argv) {
    // Endpoint can be overridden via CLI, default to TCP 5558.
    std::string endpoint = "tcp://*:5558";
    if (argc > 1) {
        endpoint = argv[1];
    }

    // Instantiate and run
    BookingWorker worker(endpoint);
    worker.run();

    return 0;
}