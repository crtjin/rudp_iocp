#include "PacketPool.h"

PacketPool::PacketPool(size_t n) : pool(n) {
    for (size_t i = 0; i + 1 < n; ++i) pool[i].next = &pool[i + 1];
    pool[n - 1].next = nullptr;
    head.store({ &pool[0], 0 }, std::memory_order_release);
}
RUDPPacket* PacketPool::allocate() {
    TaggedPtr oldh = head.load(std::memory_order_acquire);
    while (oldh.ptr) {
        RUDPPacket* next = oldh.ptr->next;
        TaggedPtr newh{ next, oldh.tag + 1 };
        if (head.compare_exchange_weak(oldh, newh,
            std::memory_order_release,
            std::memory_order_acquire)) {
            oldh.ptr->next = nullptr;
            oldh.ptr->size = 0;
            oldh.ptr->seq = 0;
            oldh.ptr->ts = 0;
            return oldh.ptr;
        }
    }
    return nullptr;
}
void PacketPool::deallocate(RUDPPacket* p) {
    TaggedPtr oldh = head.load(std::memory_order_acquire);
    TaggedPtr newh;
    do {
        p->next = oldh.ptr;
        newh.ptr = p;
        newh.tag = oldh.tag + 1;
    } while (!head.compare_exchange_weak(oldh, newh,
        std::memory_order_release,
        std::memory_order_acquire));
}