#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#pragma comment(lib, "Ws2_32.lib")

using namespace std;

constexpr int DEFAULT_PORT = 27000;
constexpr int BACKLOG = SOMAXCONN;
constexpr int MAX_DATE_SIZE = 20;

#pragma pack(push, 1)
struct TelemetryPacket
{
    int planeId;
    char time[MAX_DATE_SIZE];
    double fuel;
};
#pragma pack(pop)

enum class FlightStatus
{
    ACTIVE,
    COMPLETE,
    DISCONNECTED,
    MALFORMED
};

struct FlightRecord
{
    int planeId = -1;
    string clientIp = "";
    string firstTimestamp = "";
    string lastTimestamp = "";

    double initialFuel = 0.0;
    double currentFuel = 0.0;
    double finalFuel = 0.0;

    double runningAverageConsumption = 0.0;
    double finalAverageConsumption = 0.0;

    double cumulativeFuelConsumed = 0.0;
    int elapsedSeconds = 0;

    bool hasFirstPacket = false;
    string previousTimestamp = "";
    double previousFuel = 0.0;

    FlightStatus status = FlightStatus::ACTIVE;
};

map<int, FlightRecord> g_flights;
mutex g_flightsMutex;
mutex g_consoleMutex;
mutex g_fileMutex;

atomic<int> g_nextPlaneId{ 1 };
atomic<bool> g_serverRunning{ true };

const string FINAL_LOG_FILE = "final_flight_records.csv";

string statusToString(FlightStatus status)
{
    switch (status)
    {
    case FlightStatus::ACTIVE: return "ACTIVE";
    case FlightStatus::COMPLETE: return "COMPLETE";
    case FlightStatus::DISCONNECTED: return "DISCONNECTED";
    case FlightStatus::MALFORMED: return "MALFORMED";
    default: return "UNKNOWN";
    }
}

string currentDateTimeString()
{
    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm localTm{};
    localtime_s(&localTm, &t);

    ostringstream oss;
    oss << put_time(&localTm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

void logMessage(const string& msg)
{
    lock_guard<mutex> lock(g_consoleMutex);
    cout << "[" << currentDateTimeString() << "] " << msg << endl;
}

bool recvAll(SOCKET sock, char* buffer, int totalBytes)
{
    int received = 0;
    while (received < totalBytes)
    {
        int result = recv(sock, buffer + received, totalBytes - received, 0);
        if (result <= 0)
        {
            return false;
        }
        received += result;
    }
    return true;
}

bool sendAll(SOCKET sock, const char* buffer, int totalBytes)
{
    int sent = 0;
    while (sent < totalBytes)
    {
        int result = send(sock, buffer + sent, totalBytes - sent, 0);
        if (result == SOCKET_ERROR)
        {
            return false;
        }
        sent += result;
    }
    return true;
}

bool isPrintableTimeString(const char timeBuffer[MAX_DATE_SIZE])
{
    bool foundNull = false;

    for (int i = 0; i < MAX_DATE_SIZE; ++i)
    {
        char c = timeBuffer[i];

        if (c == '\0')
        {
            foundNull = true;
            break;
        }

        if (!(isdigit(static_cast<unsigned char>(c)) || c == '_' || c == ':' || c == ' '))
        {
            return false;
        }
    }

    return foundNull;
}

int parseElapsedSeconds(const string& previous, const string& current)
{
    tm prevTm{};
    tm currTm{};

    int pm, pd, py, ph, pmin, ps;
    int cm, cd, cy, ch, cmin, cs;

    int prevParsed = sscanf_s(previous.c_str(), "%d_%d_%d %d:%d:%d", &pm, &pd, &py, &ph, &pmin, &ps);
    int currParsed = sscanf_s(current.c_str(), "%d_%d_%d %d:%d:%d", &cm, &cd, &cy, &ch, &cmin, &cs);

    if (prevParsed != 6 || currParsed != 6)
    {
        return 1;
    }

    prevTm.tm_mon = pm - 1;
    prevTm.tm_mday = pd;
    prevTm.tm_year = py - 1900;
    prevTm.tm_hour = ph;
    prevTm.tm_min = pmin;
    prevTm.tm_sec = ps;
    prevTm.tm_isdst = -1;

    currTm.tm_mon = cm - 1;
    currTm.tm_mday = cd;
    currTm.tm_year = cy - 1900;
    currTm.tm_hour = ch;
    currTm.tm_min = cmin;
    currTm.tm_sec = cs;
    currTm.tm_isdst = -1;

    time_t prevTime = mktime(&prevTm);
    time_t currTime = mktime(&currTm);

    if (prevTime == -1 || currTime == -1)
    {
        return 1;
    }

    int diff = static_cast<int>(difftime(currTime, prevTime));
    return (diff > 0) ? diff : 1;
}

void appendFinalRecordToFile(const FlightRecord& record)
{
    lock_guard<mutex> lock(g_fileMutex);

    static bool headerWritten = false;

    ofstream out(FINAL_LOG_FILE, ios::app);
    if (!out.is_open())
    {
        logMessage("ERROR: Could not open final flight log file.");
        return;
    }

    if (!headerWritten)
    {
        out << "PlaneID,ClientIP,FirstTimestamp,LastTimestamp,ElapsedSeconds,InitialFuel,FinalFuel,FinalAverageConsumption,Status\n";
        headerWritten = true;
    }

    out << record.planeId << ","
        << record.clientIp << ","
        << record.firstTimestamp << ","
        << record.lastTimestamp << ","
        << record.elapsedSeconds << ","
        << fixed << setprecision(6)
        << record.initialFuel << ","
        << record.finalFuel << ","
        << record.finalAverageConsumption << ","
        << statusToString(record.status) << "\n";
}

void printFlightTable()
{
    lock_guard<mutex> consoleLock(g_consoleMutex);
    lock_guard<mutex> dataLock(g_flightsMutex);

    cout << "\n=====================================================================================\n";
    cout << left
        << setw(10) << "Plane ID"
        << setw(18) << "Elapsed (s)"
        << setw(18) << "Fuel (kg)"
        << setw(22) << "Avg Cons. (kg/s)"
        << setw(18) << "Status"
        << "\n";
    cout << "-------------------------------------------------------------------------------------\n";

    for (const auto& [id, rec] : g_flights)
    {
        cout << left
            << setw(10) << rec.planeId
            << setw(18) << rec.elapsedSeconds
            << setw(18) << fixed << setprecision(3) << rec.currentFuel
            << setw(22) << fixed << setprecision(6) << rec.runningAverageConsumption
            << setw(18) << statusToString(rec.status)
            << "\n";
    }

    cout << "=====================================================================================\n\n";
}

void displayThreadFunc()
{
    while (g_serverRunning)
    {
        this_thread::sleep_for(chrono::seconds(2));
        printFlightTable();
    }
}

void initializeFlightRecord(int planeId, const string& clientIp)
{
    lock_guard<mutex> lock(g_flightsMutex);

    FlightRecord record;
    record.planeId = planeId;
    record.clientIp = clientIp;
    record.status = FlightStatus::ACTIVE;

    g_flights[planeId] = record;
}

void finalizeFlight(int planeId, FlightStatus finalStatus)
{
    FlightRecord finalCopy;

    {
        lock_guard<mutex> lock(g_flightsMutex);
        auto it = g_flights.find(planeId);
        if (it == g_flights.end())
        {
            return;
        }

        it->second.status = finalStatus;
        it->second.finalFuel = it->second.currentFuel;
        it->second.finalAverageConsumption = it->second.runningAverageConsumption;
        finalCopy = it->second;
    }

    appendFinalRecordToFile(finalCopy);
}

// ======================================================
// ALBERT SECTION START
// ======================================================

void updateFlightFromPacket(const TelemetryPacket& packet)
{
    string timestamp(packet.time);

    lock_guard<mutex> lock(g_flightsMutex);

    auto it = g_flights.find(packet.planeId);
    if (it == g_flights.end())
    {
        return;
    }

    FlightRecord& rec = it->second;

    if (!rec.hasFirstPacket)
    {
        rec.hasFirstPacket = true;
        rec.firstTimestamp = timestamp;
        rec.lastTimestamp = timestamp;
        rec.initialFuel = packet.fuel;
        rec.currentFuel = packet.fuel;
        rec.previousTimestamp = timestamp;
        rec.previousFuel = packet.fuel;
        rec.elapsedSeconds = 0;
        rec.runningAverageConsumption = 0.0;
        return;
    }

    int deltaSeconds = parseElapsedSeconds(rec.previousTimestamp, timestamp);
    double fuelConsumedSinceLast = rec.previousFuel - packet.fuel;

    if (fuelConsumedSinceLast < 0.0)
    {
        fuelConsumedSinceLast = 0.0;
    }

    rec.elapsedSeconds += deltaSeconds;
    rec.cumulativeFuelConsumed += fuelConsumedSinceLast;
    rec.currentFuel = packet.fuel;
    rec.lastTimestamp = timestamp;

    if (rec.elapsedSeconds > 0)
    {
        rec.runningAverageConsumption = rec.cumulativeFuelConsumed / rec.elapsedSeconds;
    }

    rec.previousTimestamp = timestamp;
    rec.previousFuel = packet.fuel;
}

// ======================================================
// ALBERT SECTION END
// ======================================================

void clientHandler(SOCKET clientSocket, sockaddr_in clientAddr)
{
    char ipBuffer[INET_ADDRSTRLEN] = {};
    inet_ntop(AF_INET, &(clientAddr.sin_addr), ipBuffer, INET_ADDRSTRLEN);
    string clientIp = ipBuffer;

    // Assign unique ID
    // Support many clients with one handler thread each
    int planeId = g_nextPlaneId.fetch_add(1);

    logMessage("Client connected from " + clientIp + ". Assigned Plane ID = " + to_string(planeId));
    initializeFlightRecord(planeId, clientIp);

    int assignedIdNetworkOrder = htonl(planeId);
    if (!sendAll(clientSocket, reinterpret_cast<const char*>(&assignedIdNetworkOrder), sizeof(assignedIdNetworkOrder)))
    {
        logMessage("ERROR: Failed to send assigned ID to client " + clientIp);
        finalizeFlight(planeId, FlightStatus::DISCONNECTED);
        closesocket(clientSocket);
        return;
    }

    while (true)
    {
        TelemetryPacket packet{};
        bool ok = recvAll(clientSocket, reinterpret_cast<char*>(&packet), sizeof(packet));

        if (!ok)
        {
            logMessage("Client disconnected unexpectedly. Plane ID = " + to_string(planeId));
            finalizeFlight(planeId, FlightStatus::DISCONNECTED);
            break;
        }

        if (packet.planeId != planeId)
        {
            logMessage("Malformed packet rejected: ID mismatch for Plane ID = " + to_string(planeId));
            continue;
        }

        if (!isPrintableTimeString(packet.time))
        {
            logMessage("Malformed packet rejected: invalid timestamp for Plane ID = " + to_string(planeId));
            continue;
        }

        if (packet.fuel < 0.0)
        {
            logMessage("Malformed packet rejected: negative fuel for Plane ID = " + to_string(planeId));
            continue;
        }

        // ============================
        // ALBERT FUnction called here
        // parse packet + calculate/store fuel consumption
        // ============================
        updateFlightFromPacket(packet);
    }

    closesocket(clientSocket);
}

int main(int argc, char* argv[])
{
    string bindIp = "0.0.0.0";
    int port = DEFAULT_PORT;

    if (argc >= 2)
    {
        bindIp = argv[1];
    }

    if (argc >= 3)
    {
        port = atoi(argv[2]);
        if (port <= 0)
        {
            cout << "ERROR: Invalid port." << endl;
            return 1;
        }
    }

    WSADATA wsaData{};
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        cout << "ERROR: WSAStartup failed." << endl;
        return 2;
    }

    SOCKET listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket == INVALID_SOCKET)
    {
        cout << "ERROR: Failed to create listening socket." << endl;
        WSACleanup();
        return 3;
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(static_cast<u_short>(port));

    if (inet_pton(AF_INET, bindIp.c_str(), &serverAddr.sin_addr) != 1)
    {
        cout << "ERROR: Invalid bind IP address." << endl;
        closesocket(listenSocket);
        WSACleanup();
        return 4;
    }

    if (bind(listenSocket, reinterpret_cast<sockaddr*>(&serverAddr), sizeof(serverAddr)) == SOCKET_ERROR)
    {
        cout << "ERROR: bind() failed." << endl;
        closesocket(listenSocket);
        WSACleanup();
        return 5;
    }

    if (listen(listenSocket, BACKLOG) == SOCKET_ERROR)
    {
        cout << "ERROR: listen() failed." << endl;
        closesocket(listenSocket);
        WSACleanup();
        return 6;
    }

    logMessage("Server started.");
    logMessage("Listening on IP " + bindIp + ", port " + to_string(port));

    thread displayThread(displayThreadFunc);

    while (true)
    {
        sockaddr_in clientAddr{};
        int clientAddrLen = sizeof(clientAddr);

        SOCKET clientSocket = accept(listenSocket, reinterpret_cast<sockaddr*>(&clientAddr), &clientAddrLen);
        if (clientSocket == INVALID_SOCKET)
        {
            logMessage("ERROR: accept() failed, continuing...");
            continue;
        }

        thread handler(clientHandler, clientSocket, clientAddr);
        handler.detach();
    }

    g_serverRunning = false;
    if (displayThread.joinable())
    {
        displayThread.join();
    }

    closesocket(listenSocket);
    WSACleanup();
    return 0;
}