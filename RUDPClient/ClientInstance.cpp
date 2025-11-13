#include "ClientInstance.h"

ClientInstance::ClientInstance(int id_, const char* serverIp, int serverPort,
    int messages, int sendInterval,
    double sendLoss, double ackLoss)
    : id(id_), sender(nullptr), running(true),
    ackLossRate(ackLoss), messagesToSend(messages), sendIntervalMs(sendInterval)
{
    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == INVALID_SOCKET) {
        int e = WSAGetLastError();
        throw std::runtime_error("socket failed: " + std::to_string(e));
    }

    // bind ephemeral local port (optional)
    sockaddr_in local{}; local.sin_family = AF_INET; local.sin_port = htons(0); local.sin_addr.s_addr = INADDR_ANY;
    if (bind(sock, (sockaddr*)&local, sizeof(local)) == SOCKET_ERROR) {
        closesocket(sock);
        throw std::runtime_error("bind failed");
    }

    sender = new ReliableSender(sock, serverIp, serverPort, sendLoss);

    // receiver thread: listens for data and ACKs
    recvThread = std::thread([this]() {
        char buf[1024];
        sockaddr_in from{}; int fromlen = sizeof(from);
        // set recv timeout so thread can exit gracefully
        timeval tv; tv.tv_sec = 0; tv.tv_usec = 200000; // 200ms
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv, sizeof(tv));

        while (running) {
            int r = recvfrom(sock, buf, sizeof(buf), 0, (sockaddr*)&from, &fromlen);
            if (r == SOCKET_ERROR) {
                int err = WSAGetLastError();
                if (err == WSAETIMEDOUT) {
                    continue;
                }
                else {
                    // other errors -> break
                    //std::cerr << "[Client recv] error " << err << "\n";
                    continue;
                }
            }
            if (r == 4) {
                // ACK (4 bytes)
                if (simulateLoss(ackLossRate)) {
                    // simulated drop of ACK
                    //cout << "[Client] simulated DROP-ACK\n";
                    continue;
                }
                uint32_t ackn; memcpy(&ackn, buf, 4); ackn = ntohl(ackn);
                sender->handleAck(ackn);
                //cout << "[Client] ACK recv seq=" << ackn << "\n";
            }
            else if (r >= 4) {
                // server data (server->client reliable send)
                uint32_t seq; memcpy(&seq, buf, 4); seq = ntohl(seq);
                int payload = r - 4;
                // send ACK back to server for server->client packet
                uint32_t ackn = htonl(seq);
                // simulate possible ACK loss for server-side sends? keep same ackLossRate
                if (!simulateLoss(ackLossRate)) {
                    sendto(sock, (char*)&ackn, 4, 0, (sockaddr*)&from, fromlen);
                }
                // print server message
                std::string txt(buf + 4, payload);
                std::cout << "[Client " << id << "] RECV from server seq=" << seq << " msg=\"" << txt << "\"\n";
            }
        }
        });

    // retransmit thread
    retransThread = std::thread([this]() {
        sender->retransmitLoop();
        });

}
void ClientInstance::runSendLoop() {
    for (int i = 1; i <= messagesToSend; ++i) {
        std::string msg = "C" + std::to_string(id) + " msg " + std::to_string(i);
        sender->sendData(msg);
        std::this_thread::sleep_for(std::chrono::milliseconds(sendIntervalMs));
    }
}

void ClientInstance::stop() {
    running = false;
    sender->stop();
    if (recvThread.joinable()) recvThread.join();
    if (retransThread.joinable()) retransThread.join();
    if (sender) delete sender;
    if (sock != INVALID_SOCKET) { closesocket(sock); sock = INVALID_SOCKET; }
}

size_t ClientInstance::pending()
{
    return sender ? sender->pendingCount() : 0;
}