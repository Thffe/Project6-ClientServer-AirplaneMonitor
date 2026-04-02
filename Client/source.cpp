#include <windows.networking.sockets.h>
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <chrono>
#include <random>
using namespace std;
//max date length is 19, 20 for string terminator
#define MAX_DATE_SIZE 20
#define PACKET_SIZE MAX_DATE_SIZE + sizeof(double)
#pragma comment(lib, "Ws2_32.lib")
/*
Ella Kubica
Client for Project 6 Part 2
CSCN73060
*/
struct packet {
	char time[MAX_DATE_SIZE];
	double fuel;
};

void convertStringtoCharArr(string s, char* c, int arraysize) {
	int i = 0;
	while(i < s.length() && i < arraysize) {
		c[i] = s.at(i);
		i++;
	}
	if (i >= arraysize) {
		c[i - 1] = '\0';
	}
	else {
		c[i] = '\0';
	}
}

int main(int argc, char* argv[]) {
	char* serverIP;
	char localhostIP[] = "127.0.0.1";
	//255.255.255.255
	//if an IP address is provided
	if (argc >= 2) {
		serverIP = argv[1];
	}
	//if no arguement is provided
	else {
		serverIP = localhostIP;
	}

	int filenum = 0;
	random_device rd;
	mt19937 gen(rd());
	uniform_int_distribution<> uid(0, 3);
	filenum = uid(gen);
	
	string filename = "";
	switch (filenum) {
		case 0:
			filename = "katl-kefd-B737-700.txt";
			break;
		case 1:
			filename = "Telem_2023_3_12 14_56_40.txt";
			break;
		case 2:
			filename = "Telem_2023_3_12 16_26_4.txt";
			break;
		case 3:
			filename = "Telem_czba-cykf-pa28-w2_2023_3_1 12_31_27.txt";
			break;
		default:
			filename = "Error";
			break;
	}

	cout << "Opening file: " << filename << endl;
	ifstream file(filename);
	if (!file.is_open()) {
		cout << "ERROR: File could not be found" << endl;
		return 1;
	}

	//starts Winsock DLLs
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) == SOCKET_ERROR) {
		std::cout << "ERROR: Failed to start WSA" << std::endl;
		return 2;
	}
	//create client socket
	SOCKET ClientSocket;
	ClientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (ClientSocket == INVALID_SOCKET) {
		WSACleanup();
		std::cout << "ERROR: Failed to create Client Socket" << std::endl;
		return 3;
	}
	//Connect socket to specified server
	sockaddr_in SvrAddr;
	SvrAddr.sin_family = AF_INET; //Address family type itnernet
	SvrAddr.sin_port = htons(27000); //port (host to network conversion)
	SvrAddr.sin_addr.s_addr = inet_addr(serverIP); //IP address
	int sizeAddr = sizeof(SvrAddr);

	cout << "Looking for Connection at " << serverIP << endl;
	if (connect(ClientSocket, (struct sockaddr*)&SvrAddr, sizeof(SvrAddr)) == SOCKET_ERROR) {
		cout << "ERROR: Failed to connect" << endl;
		return 4;
	}


	cout << "Sending..." << endl;

	string line;
	getline(file, line);

	//cutting off row headers
	line = line.substr(line.find(",")+1);
	int comma = line.find(",");

	//substr(starting index inclusive, ending index exclusive)
	string date = line.substr(0, comma);
	
	packet sendPkt;
	convertStringtoCharArr(date, sendPkt.time, MAX_DATE_SIZE);
	

	string fuelText = line.substr(comma + 1);
	//cut off characters after fuel number
	fuelText = fuelText.substr(0, fuelText.length() - 2);
	double fuelnum = stod(fuelText);
	sendPkt.fuel = fuelnum;

	send(ClientSocket, (char*)&sendPkt, PACKET_SIZE, 0);
	

	while (!file.eof()) {
		getline(file, line);
		if (line != " "){ //ignore empty line(s)
			//cutting off beginnning comma
			line = line.substr(1);

			comma = line.find(",");
			date = line.substr(0, comma);
			convertStringtoCharArr(date, sendPkt.time, MAX_DATE_SIZE);


			fuelText = line.substr(comma + 1);
			//cut off characters after fuel number
			fuelText = fuelText.substr(0, fuelText.length() - 2);
			double fuelnum = stod(fuelText);
			sendPkt.fuel = fuelnum;

			//wait for 1 second
			this_thread::sleep_for(chrono::seconds(1));

			send(ClientSocket, (char*)&sendPkt, PACKET_SIZE, 0);
		}
	}
	cout << "Done Sending" << endl;
	closesocket(ClientSocket);
	WSACleanup();
	return 0;
}