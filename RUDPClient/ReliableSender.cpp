#include "ReliableSender.h"

ReliableSender::ReliableSender(SOCKET s, const char* ip, int port, double lossRate, int rto)
    : sock(s), nextSeq(1), running(true), sendLossRate(lossRate), rto_ms(rto)
{
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &serverAddr.sin_addr);
}

ReliableSender::~ReliableSender() {
    running = false;
}
// 보낸 데이터 만들기
void ReliableSender::sendData(const std::string& msg) {
    uint32_t seq = nextSeq.fetch_add(1);
    {
        std::lock_guard<std::mutex> lk(mtx);
        pending[seq] = { msg, now_ms(), false };
    }
    sendPacket(seq);
}

void ReliableSender::sendPacket(uint32_t seq) {
    std::lock_guard<std::mutex> lk(mtx);
    auto it = pending.find(seq);
    if (it == pending.end()) return;
    std::string& msg = it->second.data;

    // framing: 4 bytes seq (network) + payload bytes
    std::vector<char> buf(4 + msg.size());
    uint32_t nseq = htonl(seq);
    memcpy(buf.data(), &nseq, 4);
    if (!msg.empty()) memcpy(buf.data() + 4, msg.data(), msg.size());

    if (!simulateLoss(sendLossRate)) {
        int ret = sendto(sock, buf.data(), (int)buf.size(), 0, (sockaddr*)&serverAddr, sizeof(serverAddr));
        if (ret == SOCKET_ERROR) {
            int e = WSAGetLastError();
            std::cerr << "[sendto] error " << e << "\n";
        }
        else {
            //log
            //cout << "[SEND] seq=" << seq << " len=" << buf.size() << "\n";
        }
    }
    else {
        // simulated drop
        //cout << "[DROP-SEND] seq=" << seq << "\n";
    }
    it->second.lastSend = now_ms();
}

// called by receiver thread when ACK arrives
void ReliableSender::handleAck(uint32_t ackSeq) {
    std::lock_guard<std::mutex> lk(mtx);
    auto it = pending.find(ackSeq);
    if (it != pending.end()) {
        it->second.acked = true;
        pending.erase(it);
        //cout << "[ACK] seq=" << ackSeq << " freed\n";
    }
}

// retransmit loop (run in background thread)
void ReliableSender::retransmitLoop() {
    while (running) {
        std::this_thread::sleep_for(50ms);
        uint64_t t = now_ms();
        std::vector<uint32_t> toRetrans;
        {
            std::lock_guard<std::mutex> lk(mtx);
            for (auto& kv : pending) {
                uint32_t seq = kv.first;
                auto& p = kv.second;
                if (!p.acked && (t - p.lastSend) > (uint64_t)rto_ms) {
                    toRetrans.push_back(seq);
                }
            }
        }
        for (uint32_t s : toRetrans) {
            // retransmit
            //std::cout << "[RETRANS] seq=" << s << "\n";
            sendPacket(s);
        }
    }
}

void ReliableSender::stop() { running = false; }

size_t ReliableSender::pendingCount() {
    std::lock_guard<std::mutex> lk(mtx);
    return pending.size();
}