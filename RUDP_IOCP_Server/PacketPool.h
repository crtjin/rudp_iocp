#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>

#include <iostream>
#include <vector>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <sstream>

#pragma comment(lib, "ws2_32.lib")

using namespace std::chrono;
using namespace std;

// ------------------ Packet Pool ------------------
struct RUDPPacket {
    RUDPPacket* next;
    uint32_t seq;       // 시퀸스 번호
    uint64_t ts;
    int size;
    char data[512] = { 0, };
};

struct TaggedPtr {
    RUDPPacket* ptr;
    uint64_t tag;   // ABA 문제 방지용 태그
    // A스레드에서 ptr을 읽고
    // B스레드에서 ptr이 변경된 후 
    // 다시 A스레드에서 ptr을 변경하는 경우를 방지
};
static_assert(std::is_trivially_copyable<TaggedPtr>::value, "TaggedPtr must be trivially copyable");

class PacketPool {
    std::atomic<TaggedPtr> head;
    std::vector<RUDPPacket> pool;
public:
    PacketPool(size_t n);
    RUDPPacket* allocate();
    void deallocate(RUDPPacket* p);
};