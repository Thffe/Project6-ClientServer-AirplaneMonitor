#include <windows.networking.sockets.h>
#include <iostream>
#include <fstream>
#include <string>
#include <list>
using namespace std;
#define PACKET_SIZE 40
#pragma comment(lib, "Ws2_32.lib")

void convertStringtoCharArr(string s, char* c) {
	int i = 0;
	while(i < s.length() && i < PACKET_SIZE) {
		c[i] = s.at(i);
		i++;
	}
	if (i >= PACKET_SIZE) {
		c[i - 1] = '\0';
	}
	else {
		c[i] = '\0';
	}
}

void main() {


	ifstream file("katl-kefd-B737-700.txt");
	if (!file.is_open()) {
		cout << "ERROR: File could not be found" << endl;
		return;
	}

	//starts Winsock DLLs
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		std::cout << "ERROR: Failed to start WSA" << std::endl;
		return;
	}
	//create client socket
	SOCKET ServerSocket;
	ServerSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (ServerSocket == INVALID_SOCKET) {
		WSACleanup();
		std::cout << "ERROR: Failed to create Server Socket" << std::endl;
		return;
	}
	//Connect socket to specified server
	sockaddr_in SvrAddr;
	SvrAddr.sin_family = AF_INET; //Address family type itnernet
	SvrAddr.sin_port = htons(27000); //port (host to network conversion)
	SvrAddr.sin_addr.s_addr = inet_addr("127.0.0.1"); //IP address
	int sizeAddr = sizeof(SvrAddr);


	string line;
	getline(file, line);
	//cutting off padding
	string substring = line.substr(line.find(",")+1);
	char sendbuffer[PACKET_SIZE];
	convertStringtoCharArr(substring, sendbuffer);

	while (!file.eof()) {
		//cout << sendbuffer << endl;
		sendto(ServerSocket, sendbuffer, PACKET_SIZE, 0, (struct sockaddr*)&SvrAddr, sizeAddr);

		getline(file, line);
		//cutting off padding
		line = line.substr(1);
		convertStringtoCharArr(line, sendbuffer);
	}
	/*
	char recvbuffer[14];
	recvfrom(ServerSocket, recvbuffer, 14, 0, (struct sockaddr*)&SvrAddr, &sizeAddr);
	*/
	closesocket(ServerSocket);
	WSACleanup();
	
}