#include "RUDP_IOCP_Server.h"
#include "NetworkEnum.h"

RUDP_IOCP_Server::RUDP_IOCP_Server(int port, int recvOverlap, size_t poolSize)
    : pool(poolSize), recvOverlapCount(recvOverlap)
{
    // socket
    sock = WSASocketW(AF_INET, SOCK_DGRAM, IPPROTO_UDP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (sock == INVALID_SOCKET) { throw runtime_error("socket failed"); }

    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(sock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        closesocket(sock); throw runtime_error("bind failed");
    }

    // create IOCP and associate socket (KEY: pointer to server instance)
    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, (ULONG_PTR)this, 0);
    if (!iocp) { closesocket(sock); throw runtime_error("CreateIoCompletionPort failed"); }
    if (!CreateIoCompletionPort((HANDLE)sock, iocp, (ULONG_PTR)this, 0)) {
        closesocket(sock); CloseHandle(iocp); throw runtime_error("Associate socket with IOCP failed");
    }

    // post receives
    for (int i = 0; i < recvOverlapCount; ++i) {
        postRecv();
    }

    // start completion loop thread
    completionThread = std::thread(&RUDP_IOCP_Server::completionLoop, this);

    // retransmit thread
    retransmitThread = std::thread(&RUDP_IOCP_Server::retransmitLoop, this);

    std::cout << "[Server] started on port " << port << "\n";
}

RUDP_IOCP_Server::~RUDP_IOCP_Server()
{
    stop();
}
void RUDP_IOCP_Server::stop()
{
    if (!running.exchange(false)) return;
    // Post a shutdown completion by posting to IOCP
    PostQueuedCompletionStatus(iocp, 0, (ULONG_PTR)nullptr, nullptr);
    if (completionThread.joinable()) completionThread.join();
    if (retransmitThread.joinable()) retransmitThread.join();
    if (sock != INVALID_SOCKET) { closesocket(sock); sock = INVALID_SOCKET; }
    if (iocp) { CloseHandle(iocp); iocp = nullptr; }
}
uint64_t RUDP_IOCP_Server::now_ms()
{
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}
// 랜덤 난수 생성기로 손실 시뮬레이션
bool RUDP_IOCP_Server::simulateLoss(double lossRate)
{
    static thread_local std::mt19937 gen((uint32_t)std::chrono::steady_clock::now().time_since_epoch().count());
    std::bernoulli_distribution d(lossRate);    // 브루노이 분포
    return d(gen);
}
// 주소를 문자열로 변환
string RUDP_IOCP_Server::addrToStr(const SOCKADDR_IN& a)
{
    char ip[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &a.sin_addr, ip, sizeof(ip));
    return string(ip) + ":" + to_string(ntohs(a.sin_port));
}
// UDP Recv처리
void RUDP_IOCP_Server::postRecv()
{
    IoContext* ctx = new IoContext();
    ctx->op = IoContext::RECV;
    ctx->wbuf.buf = ctx->buffer; ctx->wbuf.len = sizeof(ctx->buffer);
    ctx->fromlen = sizeof(ctx->from);
    DWORD flags = 0;
    DWORD bytes = 0;
    int rc = WSARecvFrom(sock, &ctx->wbuf, 1, &bytes, &flags,
        (SOCKADDR*)&ctx->from, &ctx->fromlen, &ctx->overlapped, nullptr);
    if (rc == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err != WSA_IO_PENDING) {
            std::cerr << "[postRecv] WSARecvFrom failed err=" << err << "\n";
            delete ctx;
        }
    }
}
// session 생성 or 조회
std::shared_ptr<Session> RUDP_IOCP_Server::getSession(const SOCKADDR_IN& from)
{
    string key = addrToStr(from);
    {
        std::lock_guard<std::mutex> lk(sessionsMtx);
        auto it = sessions.find(key);
        if (it != sessions.end()) return it->second;
        // new session
        auto s = std::make_shared<Session>();
        s->addr = from;
        sessions[key] = s;
        std::cout << "[NewSession] " << key << "\n";
        return s;
    }
}

// UDP Send처리
void RUDP_IOCP_Server::sendRaw(const SOCKADDR_IN& to, const char* buf, int len)
{
    IoContext* ctx = new IoContext();
    ctx->op = IoContext::SEND;
    ctx->fromlen = sizeof(ctx->from);
    memcpy(&ctx->from, &to, sizeof(to));
    memcpy(ctx->buffer, buf, len);
    ctx->wbuf.buf = ctx->buffer; ctx->wbuf.len = len;

    DWORD bytes = 0;
    int rc = WSASendTo(sock, &ctx->wbuf, 1, &bytes, 0, (SOCKADDR*)&ctx->from, ctx->fromlen, &ctx->overlapped, nullptr);
    if (rc == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err != WSA_IO_PENDING) {
            std::cerr << "[sendRaw] WSASendTo failed err=" << err << "\n";
            delete ctx;
        }
    }
}

// reliable send: allocate packet, send, and put into session.pending
void RUDP_IOCP_Server::sendReliable(shared_ptr<Session> sess, const string& msg)
{
    RUDPPacket* pkt = pool.allocate();
    if (!pkt) { std::cerr << "[sendReliable] pool empty\n"; return; }
    {
        std::lock_guard<std::mutex> lk(sess->mtx);
        pkt->seq = sess->nextSeq++;
        pkt->ts = now_ms();
        pkt->size = min((int)msg.size(), (int)sizeof(pkt->data));
        memcpy(pkt->data, msg.data(), pkt->size);

		// Ack 패킷 생성
        char out[128] = { 0, };
        uint32_t seqn = htonl(pkt->seq);
        memcpy(out, &seqn, 4);
        memcpy(out + 4, pkt->data, pkt->size);
        int outlen = 4 + pkt->size;

		// 실패 시뮬레이션
        if (!simulateLoss(0.12)) {
            // Ack 전송
            sendRaw(sess->addr, out, outlen);
            // record pending
            PendingEntry e{ pkt, now_ms(), false };
            sess->pending[pkt->seq] = e;
        }
        else {
            // simulated drop: we still count as sent (but didn't actually send)
            std::cout << "[SimDrop sendReliable] to " << addrToStr(sess->addr) << " seq=" << pkt->seq << "\n";
            PendingEntry e{ pkt, now_ms(), false };
            sess->pending[pkt->seq] = e;
        }
    }
    std::cout << "[sendReliable] to " << addrToStr(sess->addr) << " seq=" << pkt->seq << " msg=" << string(pkt->data, pkt->size) << "\n";
}

// 완료 이벤트 처리
void RUDP_IOCP_Server::completionLoop()
{
    while (running) {
        DWORD bytes = 0;
        ULONG_PTR key = 0;
        OVERLAPPED* ov = nullptr;
        BOOL ok = GetQueuedCompletionStatus(iocp, &bytes, &key, &ov, INFINITE);
        if (!running) break;

        if (!ov) {
            // shutdown or spurious, break
            if (!ok) { /* continue? */ }
            break;
        }

        IoContext* ctx = CONTAINING_RECORD(ov, IoContext, overlapped);
        if (!ok) {
            int err = GetLastError();
            // For simplicity, we log and repost recv or free
            if (ctx->op == IoContext::RECV) {
                delete ctx;
                postRecv();
                continue;
            }
            else {
                delete ctx;
                continue;
            }
        }

        if (ctx->op == IoContext::RECV) {
            // bytes bytes received; from = ctx->from
            if (bytes >= 4) 
            {
                // check if this is ACK-only (4 bytes) or data(4+payload)
                if (bytes == 4) 
                {
                    uint32_t ackn; memcpy(&ackn, ctx->buffer, 4); ackn = ntohl(ackn);
                    // find session for from
                    auto sess = getSession(ctx->from);
                    // treat as ACK for server->client reliable sends
                    {
                        std::lock_guard<std::mutex> lk(sess->mtx);
                        auto it = sess->pending.find(ackn);
                        if (it != sess->pending.end()) {
                            it->second.acked = true;
                            pool.deallocate(it->second.pkt);
                            sess->pending.erase(it);
                            std::cout << "[ACK recv] from " << addrToStr(ctx->from) << " seq=" << ackn << "\n";
                        }
                    }
                }
                else {
                    // data packet: first 4 bytes seq, rest payload
                    uint32_t seqn; memcpy(&seqn, ctx->buffer, 4); seqn = ntohl(seqn);
                    int payload = (int)bytes - 4;
                    std::cout << "[DATA recv] from " << addrToStr(ctx->from) << " seq=" << seqn << " payload=" << payload << "\n";

                    // send ACK back (unreliable ACK)
                    uint32_t ack_out = htonl(seqn);
                    sendRaw(ctx->from, (char*)&ack_out, 4);

                    // find session & optionally respond reliably
                    auto sess = getSession(ctx->from);
					sess->lastRecv = now_ms();
                    // echo back reliably
                    sendReliable(sess, string("SrvEcho to seq ") + to_string(seqn));
                }
            }

            // repost recv
            delete ctx;
            postRecv();
        }
        // else if (ctx->op == IoContext::SEND) {
        //    delete ctx;
        // }
        else {
            delete ctx;
        }
    } // end while
}

// 재전송 루프
void RUDP_IOCP_Server::retransmitLoop()
{
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        uint64_t t = now_ms();
        std::vector<pair<shared_ptr<Session>, uint32_t>> toRetrans;        
        {
            std::lock_guard<std::mutex> lk(sessionsMtx);
            std::vector<std::string> removeSessions;
            for (auto& kv : sessions) 
            {
                auto s = kv.second;
                std::lock_guard<std::mutex> lk2(s->mtx);
                bool bremoveCheck = false;
                for (auto& p : s->pending) {
                    uint32_t seq = p.first;
                    PendingEntry& e = p.second;
                    if (!e.acked && t - e.lastSend > 400) {
                        e.repeat++;

                        if( e.repeat > _MAX_REPEAT_ ) {
                            std::cout << "[GiveUp retransmit] to " << addrToStr(s->addr) << " seq=" << seq << "\n";
							removeSessions.push_back(kv.first);
                            bremoveCheck = true;
                            break;
						}
                        else {
                            // schedule retransmit
                            toRetrans.emplace_back(s, seq);
                        }                        
                    }
                }
				// if session is already marked for removal, skip idle check
                if( false == bremoveCheck ) 
                {
                    // also check idle timeout
                    if (t - s->lastRecv > _LAST_RECV_) 
                    {
                        std::cout << "[SessionTimeout] " << addrToStr(s->addr) << "\n";
                        removeSessions.push_back(kv.first);
					}
                }
            }
            for(std::string val : removeSessions)
            {
                sessions.erase( val );
			}

        }

        for (auto& pr : toRetrans) {
            auto s = pr.first;
            uint32_t seq = pr.second;
            std::lock_guard<std::mutex> lk(s->mtx);
            auto it = s->pending.find(seq);
            if (it != s->pending.end() && !it->second.acked) {
                // resend packet raw
                RUDPPacket* pkt = it->second.pkt;
                char out[600];
                uint32_t seqn = htonl(pkt->seq);
                memcpy(out, &seqn, 4);
                memcpy(out + 4, pkt->data, pkt->size);
                int len = 4 + pkt->size;
                if (!simulateLoss(0.12)) {
                    sendRaw(s->addr, out, len);
                }
                else {
                    std::cout << "[SimDrop retransmit] to " << addrToStr(s->addr) << " seq=" << pkt->seq << "\n";
                }
                it->second.lastSend = now_ms();
                std::cout << "[Retransmitted] to " << addrToStr(s->addr) << " seq=" << pkt->seq << "\n";
            }
        }
    }
}