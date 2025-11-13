#include "ClientInstance.h"

int main(int argc, char** argv) {
    // usage: rudp_multi_client_win.exe [client_count] [server_ip] [server_port] [msgs_per_client]
    int clientCount = 3;
    const char* serverIp = "127.0.0.1";
    int serverPort = 40010;
    int msgsPerClient = 10;
    int sendIntervalMs = 120;

    if (argc >= 2) clientCount = atoi(argv[1]);
    if (argc >= 3) serverIp = argv[2];
    if (argc >= 4) serverPort = atoi(argv[3]);
    if (argc >= 5) msgsPerClient = atoi(argv[4]);
    if (argc >= 6) sendIntervalMs = atoi(argv[5]);

    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cerr << "WSAStartup failed\n";
        return -1;
    }

    std::cout << "Starting " << clientCount << " client instances -> server " << serverIp << ":" << serverPort << "\n";

    std::vector<std::unique_ptr<ClientInstance>> clients;
    try {
        for (int i = 0; i < clientCount; ++i) {
            clients.emplace_back(new ClientInstance(i + 1, serverIp, serverPort, msgsPerClient, sendIntervalMs));
        }

        // start send loops on each client in separate threads (so they run concurrently)
        std::vector<std::thread> senders;
        for (int i = 0; i < clientCount; ++i) {
            senders.emplace_back(&ClientInstance::runSendLoop, clients[i].get());
        }

        // wait for senders to complete
        for (auto& t : senders) if (t.joinable()) t.join();

        // after sending, give some time for retransmits/acks to settle
        std::cout << "All clients finished sending. Waiting for pending ACKs and retransmissions...\n";
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // print pending counts
        for (int i = 0; i < clientCount; ++i) {
            std::cout << "Client " << (i + 1) << " pending: " << clients[i]->pending() << "\n";
        }

        // stop clients
        for (auto& c : clients) c->stop();
    }
    catch (const std::exception& ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }

    WSACleanup();
    std::cout << "Done.\n";
    return 0;
}
