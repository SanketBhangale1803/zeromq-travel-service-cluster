#ifndef WORKER_BASE_HPP
#define WORKER_BASE_HPP

#include "zmq.hpp"
#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

using json = nlohmann::json;

class WorkerBase {
protected:
    std::string m_endpoint;
    std::string m_name;
    zmq::context_t m_ctx;
    zmq::socket_t m_socket;

    // Shared Helper: Available to all subclasses
    std::vector<std::string> split(const std::string &s, char delim) {
        std::vector<std::string> parts;
        std::string current;
        for (char c : s) {
            if (c == delim) {
                parts.push_back(current);
                current.clear();
            } else {
                current.push_back(c);
            }
        }
        parts.push_back(current);
        return parts;
    }

public:
    WorkerBase(const std::string& endpoint, const std::string& name) 
        : m_endpoint(endpoint), m_name(name), m_ctx(1), m_socket(m_ctx, zmq::socket_type::rep) 
    {
        m_socket.bind(m_endpoint);
        std::cout << "[" << m_name << "] Listening on " << m_endpoint << std::endl;
    }

    virtual ~WorkerBase() = default;

    // Pure virtual function: Subclasses MUST implement this.
    // Input: Raw request string. Output: Raw JSON response string.
    virtual std::string process_request(const std::string& request) = 0;

    // The Main Event Loop (The Template Method)
    void run() {
        while (true) {
            zmq::message_t req_msg;
            try {
                auto result = m_socket.recv(req_msg, zmq::recv_flags::none);
                if (!result) continue;
            } catch (const zmq::error_t &e) {
                std::cerr << "[" << m_name << "] recv error: " << e.what() << std::endl;
                continue;
            }

            std::string req(static_cast<char *>(req_msg.data()), req_msg.size());
            std::cout << "[" << m_name << "] Received: " << req << std::endl;

            // Delegate business logic to the specific worker implementation
            std::string resp = process_request(req);

            zmq::message_t reply(resp.size());
            memcpy(reply.data(), resp.data(), resp.size());

            try {
                m_socket.send(reply, zmq::send_flags::none);
            } catch (const zmq::error_t &e) {
                std::cerr << "[" << m_name << "] send error: " << e.what() << std::endl;
            }
        }
    }
};

#endif