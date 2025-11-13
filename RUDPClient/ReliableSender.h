#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <atomic>
#include <random>
#include <string>

#pragma comment(lib, "ws2_32.lib")

using namespace std;
using namespace std::chrono;

static std::mt19937 rng((unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count());

static bool simulateLoss(double loss_rate) {
    std::bernoulli_distribution d(loss_rate);
    return d(rng); // true = drop
}

static uint64_t now_ms() {
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// Simple ReliableSender for client
class ReliableSender {
    SOCKET sock;
    sockaddr_in serverAddr;
    std::mutex mtx;

    struct Pending {
        std::string data;
        uint64_t lastSend;
        bool acked;
    };
    std::unordered_map<uint32_t, Pending> pending;
    std::atomic<uint32_t> nextSeq;
    std::atomic<bool> running;
    double sendLossRate; // simulate sending loss
    int rto_ms;

    void sendPacket(uint32_t seq);    

public:
    ReliableSender(SOCKET s, const char* ip, int port, double lossRate, int rto = 300);

    ~ReliableSender();

    void sendData(const std::string& msg);
    void handleAck(uint32_t ackSeq);

    void retransmitLoop();

    void stop();

    size_t pendingCount();
};