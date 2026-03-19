#include <windows.networking.sockets.h>
#include <iostream>
#include <fstream>
using namespace std;
#pragma comment(lib, "Ws2_32.lib")

void main() {

	ifstream file("ff.txt");
	if (file.is_open()) {
		cout << "File could not be found" << endl;
		return;
	}

	//starts Winsock DLLs
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		std::cout << "ERROR: Failed to start WSA" << std::endl;
		return;
	}
	//create client socket
	SOCKET ClientSocket;
	ClientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (ClientSocket == INVALID_SOCKET) {
		WSACleanup();
		std::cout << "ERROR: Failed to create ClientSocket" << std::endl;
		return;
	}

	//Connect socket to specified server
	sockaddr_in CliAddr;
	CliAddr.sin_family = AF_INET; //Address family type itnernet
	CliAddr.sin_port = htons(27000); //port (host to network conversion)
	CliAddr.sin_addr.s_addr = inet_addr("127.0.0.1"); //IP address

	sockaddr_in SvrAddr;
	char sendbuffer[] = "Hello World\0";
	int sizeAddr = sizeof(SvrAddr);
	sendto(ClientSocket, sendbuffer, 13, 0, (struct sockaddr*)&CliAddr, sizeof(CliAddr));
	std::cout << "Sent." << std::endl;
	char recvbuffer[14];
	//"Hello World\0"
	std::cout << "Waiting..." << std::endl;
	recvfrom(ClientSocket, recvbuffer, 14, 0, (struct sockaddr*)&SvrAddr, &sizeAddr);
	std::cout << recvbuffer << std::endl;


	closesocket(ClientSocket);
	WSACleanup();
}