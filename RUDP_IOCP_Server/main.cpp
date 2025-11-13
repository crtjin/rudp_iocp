#include "RUDP_IOCP_Server.h"

// ------------------ main ------------------
int main() {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::cerr << "WSAStartup failed\n"; return -1;
    }

    try {
        RUDP_IOCP_Server server(40010, 32, 2048);

        // Run server for some time (demo)
        std::cout << "IOCP RUDP server running. Press Enter to stop...\n";
        std::string dummy; std::getline(std::cin, dummy);

        server.stop();
    }
    catch (const std::exception& ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }

    WSACleanup();
    return 0;
}
