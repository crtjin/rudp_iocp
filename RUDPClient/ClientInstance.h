#include "ReliableSender.h"

class ClientInstance {
    int id;
    SOCKET sock;
    ReliableSender* sender;
    std::thread recvThread;
    std::thread retransThread;
    std::atomic<bool> running;
    double ackLossRate; // simulate loss of ACKs from server (i.e., drop recv of ACK)
    int messagesToSend;
    int sendIntervalMs;

public:
    ClientInstance(int id_, const char* serverIp, int serverPort,
        int messages, int sendInterval,
        double sendLoss = 0.12, double ackLoss = 0.06);

    void runSendLoop();
    void stop();
    size_t pending();
};