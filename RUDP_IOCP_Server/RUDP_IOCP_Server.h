#include "PacketPool.h"

// ------------------ IO structures ------------------
struct IoContext {
    WSAOVERLAPPED overlapped;
    WSABUF wbuf;
    char buffer[1024] = { 0, };
    SOCKADDR_IN from;
    int fromlen;
    DWORD flags;
    enum OpType { RECV, SEND } op;
    RUDPPacket* pkt; // optional: for send ops, point to packet in pool
    IoContext() : fromlen(sizeof(from)), flags(0), op(RECV), pkt(nullptr) {
        ZeroMemory(&overlapped, sizeof(overlapped));
        ZeroMemory(&from, sizeof(from));
        wbuf.buf = buffer; wbuf.len = sizeof(buffer);
    }
};


// ------------------ Session ------------------
struct PendingEntry {
    RUDPPacket* pkt;
    uint64_t lastSend;
    bool acked; // 응답 받았다는 여부
    int repeat; // 재전송 횟수
};

struct Session {
    SOCKADDR_IN addr;
    std::mutex mtx;
    uint32_t nextSeq = 1;
    unordered_map<uint32_t, PendingEntry> pending; // server->client pending
    // sender uses same socket; no separate ReliableSender object (keeps minimal)
    uint64_t lastRecv;
};

// ------------------ Server ------------------
class RUDP_IOCP_Server {
    SOCKET sock = INVALID_SOCKET;
    HANDLE iocp = nullptr;
    PacketPool pool;
    int recvOverlapCount;
    std::unordered_map<std::string, std::shared_ptr<Session>> sessions;
    std::mutex sessionsMtx;
    std::atomic<bool> running{ true };
    std::thread completionThread;
    std::thread retransmitThread;

public:
    RUDP_IOCP_Server(int port, int recvOverlap = 16, size_t poolSize = 1024);
    ~RUDP_IOCP_Server();
    void stop();
private:
    
    uint64_t now_ms();
	// 랜덤 난수 생성기로 손실 시뮬레이션
    bool simulateLoss(double lossRate);
    // 주소를 문자열로 변환
    string addrToStr(const SOCKADDR_IN& a);

    // UDP Recv처리
    void postRecv();

    // session 생성 or 조회
    std::shared_ptr<Session> getSession(const SOCKADDR_IN& from);

    // UDP Send처리
    void sendRaw(const SOCKADDR_IN& to, const char* buf, int len);

    // reliable send: allocate packet, send, and put into session.pending
    void sendReliable(shared_ptr<Session> sess, const string& msg);

    // 완료 이벤트 처리
    void completionLoop();
    // 재전송 루프
    void retransmitLoop();
};