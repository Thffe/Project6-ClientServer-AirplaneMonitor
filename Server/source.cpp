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
#include <condition_variable>
#include <deque>


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
mutex g_fileMutex;

atomic<int> g_nextPlaneId{ 1 };
atomic<bool> g_serverRunning{ true };


//async print queue
struct PrintQueue
{
    deque<string>      items;
    mutex              mtx;
    condition_variable cv;
    bool               done = false;

    void push(string msg)
    {
        {
            lock_guard<mutex> lk(mtx);
            items.push_back(move(msg));
        }
        cv.notify_one();
    }

    void drainLoop()
    {
        while (true)
        {
            unique_lock<mutex> lk(mtx);
            cv.wait(lk, [this] { return !items.empty() || done; });

            while (!items.empty())
            {
                string msg = move(items.front());
                items.pop_front();
                lk.unlock();
                cout << msg << '\n';
                lk.lock();
            }
            if (done && items.empty()) break;
        }
    }

    void shutdown()
    {
        {
            lock_guard<mutex> lk(mtx);
            done = true;
        }
        cv.notify_one();
    }
} g_printQueue;

//graceful SHUTDOWN
SOCKET g_listenSocket = INVALID_SOCKET;

// when user hits Ctrl+C or closes window.
// Sets the global running flag to false and closes the listen socket so the
// main thread unblocks from accept() and exits the loop cleanly.
BOOL WINAPI consoleCtrlHandler(DWORD signal)
{
    if (signal == CTRL_C_EVENT || signal == CTRL_CLOSE_EVENT)
    {
        cout << "\n[SHUTDOWN] Shutdown signal received. Stopping server...\n";
        g_serverRunning = false;

        // Closing the listen socket forces accept() to return INVALID_SOCKET,
        // breaking the infinite accept loop in main().
        if (g_listenSocket != INVALID_SOCKET)
        {
            closesocket(g_listenSocket);
            g_listenSocket = INVALID_SOCKET;
        }
        return TRUE;
    }
    return FALSE;
}

//startup arg validation
bool isValidIPv4(const string& ip)
{
    sockaddr_in sa{};
    return inet_pton(AF_INET, ip.c_str(), &sa.sin_addr) == 1;
}

// Returns true if port is in the valid user/registered port range (1–65535).
bool isValidPort(int port)
{
    return port >= 1 && port <= 65535;
}


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


// SRV-USE-003: "display connection, disconnection, and error events in a
// readable format so the operator can quickly understand system status."
enum class LogLevel { INFO, WARNING, ERR };


void logMessage(const string& msg, LogLevel level = LogLevel::INFO)
{
    string tag;
    switch (level)
    {
    case LogLevel::INFO:    tag = "[INFO]   "; break;
    case LogLevel::WARNING: tag = "[WARN]   "; break;
    case LogLevel::ERR:     tag = "[ERROR]  "; break;
    }

    g_printQueue.push("[" + currentDateTimeString() + "] " + tag + msg);

}


bool recvAll(SOCKET sock, char* buffer, int totalBytes)
{
    int received = 0;
    while (received < totalBytes)
    {
        int result = recv(sock, buffer + received, totalBytes - received, 0);
        if (result <= 0)    return false;
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
        if (result == SOCKET_ERROR) return false;        
        sent += result;
    }
    return true;
}


//(SRV-FUN-007) detect malformed packet
// Returns a string describing the problem, or "" if packet is valid.
// Returning a description instead of bool lets clientHandler log the exact
// reason, satisfying SRV-USE-003 (readable error events).
string validatePacket(const TelemetryPacket& pkt, int expectedPlaneId)
{
    if (pkt.planeId != expectedPlaneId)
        return "plane ID mismatch (got " + to_string(pkt.planeId) +
        ", expected " + to_string(expectedPlaneId) + ")";

    // Check null-termination and character set
    bool foundNull = false;
    int timeLen = 0;
    for (int i = 0; i < MAX_DATE_SIZE; ++i)
    {
        char c = pkt.time[i];
        if (c == '\0') { foundNull = true; break; }
        if (!(isdigit((unsigned char)c) || c == '_' || c == ':' || c == ' ')) return "timestamp contains invalid character '" + string(1, c) + "'";
        ++timeLen;
    }
    if (!foundNull) return "timestamp missing null terminator";
    if (timeLen < 5) return "timestamp too short (" + to_string(timeLen) + " chars)";

    // Validate fuel value
    if (!isfinite(pkt.fuel)) return "fuel value is NaN or infinite";
    if (pkt.fuel < 0.0) return "fuel value is negative (" + to_string(pkt.fuel) + " kg)";
    if (pkt.fuel > 200000.0) return "fuel value implausibly large (" + to_string(pkt.fuel) + " kg)";

    return ""; // valid
}

int parseElapsedSeconds(const string& previous, const string& current)
{
    tm prevTm{};
    tm currTm{};

    int pm, pd, py, ph, pmin, ps;
    int cm, cd, cy, ch, cmin, cs;

    int prevParsed = sscanf_s(previous.c_str(), "%d_%d_%d %d:%d:%d", &pm, &pd, &py, &ph, &pmin, &ps);
    int currParsed = sscanf_s(current.c_str(), "%d_%d_%d %d:%d:%d", &cm, &cd, &cy, &ch, &cmin, &cs);

    if (prevParsed != 6 || currParsed != 6) return 1;

    prevTm.tm_mon = pm - 1; prevTm.tm_mday = pd; prevTm.tm_year = py - 1900;
    prevTm.tm_hour = ph; prevTm.tm_min = pmin; prevTm.tm_sec = ps;
    prevTm.tm_isdst = -1;

    currTm.tm_mon = cm - 1; currTm.tm_mday = cd; currTm.tm_year = cy - 1900;
    currTm.tm_hour = ch; currTm.tm_min = cmin; currTm.tm_sec = cs;
    currTm.tm_isdst = -1;

    time_t prevTime = mktime(&prevTm);
    time_t currTime = mktime(&currTm);

    if (prevTime == -1 || currTime == -1) return 1;

    int diff = static_cast<int>(difftime(currTime, prevTime));
    return (diff > 0) ? diff : 1;
}

bool logFileHasHeader()
{
    ifstream check(FINAL_LOG_FILE);
    if (!check.is_open()) return false;
    string firstLine;
    getline(check, firstLine);
    return !firstLine.empty();
}

void appendFinalRecordToFile(const FlightRecord& record)
{
    lock_guard<mutex> lock(g_fileMutex);

    bool needHeader = !logFileHasHeader();

    ofstream out(FINAL_LOG_FILE, ios::app);
    if (!out.is_open())
    {
        logMessage("Could not open final flight log file: " + FINAL_LOG_FILE, LogLevel::ERR);
        return;
    }

    if (needHeader)
    {
        out << "PlaneID,ClientIP,FirstTimestamp,LastTimestamp,"
            "ElapsedSeconds,InitialFuel_kg,FinalFuel_kg,"
            "FinalAvgConsumption_kg_s,Status\n";
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

    logMessage("Flight record saved for Plane ID " + to_string(record.planeId) +
        " | Status: " + statusToString(record.status) +
        " | Avg consumption: " + to_string(record.finalAverageConsumption) + " kg/s");
}

//
//bool isPrintableTimeString(const char timeBuffer[MAX_DATE_SIZE])
//{
//    bool foundNull = false;
//
//    for (int i = 0; i < MAX_DATE_SIZE; ++i)
//    {
//        char c = timeBuffer[i];
//
//        if (c == '\0')
//        {
//            foundNull = true;
//            break;
//        }
//
//        if (!(isdigit(static_cast<unsigned char>(c)) || c == '_' || c == ':' || c == ' '))
//        {
//            return false;
//        }
//    }
//
//    return foundNull;
//}
//
//int parseElapsedSeconds(const string& previous, const string& current)
//{
//    tm prevTm{};
//    tm currTm{};
//
//    int pm, pd, py, ph, pmin, ps;
//    int cm, cd, cy, ch, cmin, cs;
//
//    int prevParsed = sscanf_s(previous.c_str(), "%d_%d_%d %d:%d:%d", &pm, &pd, &py, &ph, &pmin, &ps);
//    int currParsed = sscanf_s(current.c_str(), "%d_%d_%d %d:%d:%d", &cm, &cd, &cy, &ch, &cmin, &cs);
//
//    if (prevParsed != 6 || currParsed != 6)
//    {
//        return 1;
//    }
//
//    prevTm.tm_mon = pm - 1;
//    prevTm.tm_mday = pd;
//    prevTm.tm_year = py - 1900;
//    prevTm.tm_hour = ph;
//    prevTm.tm_min = pmin;
//    prevTm.tm_sec = ps;
//    prevTm.tm_isdst = -1;
//
//    currTm.tm_mon = cm - 1;
//    currTm.tm_mday = cd;
//    currTm.tm_year = cy - 1900;
//    currTm.tm_hour = ch;
//    currTm.tm_min = cmin;
//    currTm.tm_sec = cs;
//    currTm.tm_isdst = -1;
//
//    time_t prevTime = mktime(&prevTm);
//    time_t currTime = mktime(&currTm);
//
//    if (prevTime == -1 || currTime == -1)
//    {
//        return 1;
//    }
//
//    int diff = static_cast<int>(difftime(currTime, prevTime));
//    return (diff > 0) ? diff : 1;
//}
//
//
////log file
////restarting the server appends correctly without a duplicate header row, and a brand-new file always gets a header.
//bool logFileHasHeader()
//{
//    ifstream check(FINAL_LOG_FILE);
//    if (!check.is_open()) return false;     // file doesn't exist yet
//    string firstLine;
//    getline(check, firstLine);
//    return !firstLine.empty();              // has content = has header
//}
//
//
//void appendFinalRecordToFile(const FlightRecord& record)
//{
//    lock_guard<mutex> lock(g_fileMutex);
//
//    bool needHeader = !logFileHasHeader();
//
//    ofstream out(FINAL_LOG_FILE, ios::app);
//    if (!out.is_open())
//    {
//        logMessage("Could not open final flight log file: " + FINAL_LOG_FILE, LogLevel::ERR);
//        return;
//    }
//
//    if (needHeader)
//    {
//        out << "PlaneID,ClientIP,FirstTimestamp,LastTimestamp,"
//            "ElapsedSeconds,InitialFuel_kg,FinalFuel_kg,"
//            "FinalAvgConsumption_kg_s,Status\n";
//    }
//
//    out << record.planeId << ","
//        << record.clientIp << ","
//        << record.firstTimestamp << ","
//        << record.lastTimestamp << ","
//        << record.elapsedSeconds << ","
//        << fixed << setprecision(6)
//        << record.initialFuel << ","
//        << record.finalFuel << ","
//        << record.finalAverageConsumption << ","
//        << statusToString(record.status) << "\n";
//
//    logMessage("Flight record saved for Plane ID " + to_string(record.planeId) +
//        " | Status: " + statusToString(record.status) +
//        " | Avg consumption: " +
//        to_string(record.finalAverageConsumption) + " kg/s");
//}


void printFlightTable()
{
    ostringstream out;
    {
        lock_guard<mutex> dataLock(g_flightsMutex);

        out << "\n================================================================"
            "============================\n";
        out << left
            << setw(10) << "PlaneID"
            << setw(18) << "Client IP"
            << setw(12) << "Time (s)"
            << setw(16) << "Init Fuel (kg)"
            << setw(16) << "Fuel Now (kg)"
            << setw(20) << "Avg Cons (kg/s)"
            << setw(14) << "Status"
            << "\n";
        out << "----------------------------------------------------------------"
            "----------------------------\n";

        if (g_flights.empty())
        {
            out << "  (no active flights)\n";
        }
        else
        {
            for (const auto& [id, rec] : g_flights)
            {
                out << left
                    << setw(10) << rec.planeId
                    << setw(18) << rec.clientIp
                    << setw(12) << rec.elapsedSeconds
                    << setw(16) << fixed << setprecision(2) << rec.initialFuel
                    << setw(16) << fixed << setprecision(2) << rec.currentFuel
                    << setw(20) << fixed << setprecision(6) << rec.runningAverageConsumption
                    << setw(14) << statusToString(rec.status)
                    << "\n";
            }
        }
        out << "================================================================"
            "============================\n";
    }
    g_printQueue.push(out.str());
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
        if (it == g_flights.end()) return;
        

        it->second.status = finalStatus;
        it->second.finalFuel = it->second.currentFuel;
        it->second.finalAverageConsumption = it->second.runningAverageConsumption;
        finalCopy = it->second;

        g_flights.erase(it);
    }

    appendFinalRecordToFile(finalCopy);
}


void updateFlightFromPacket(const TelemetryPacket& packet)
{
    string timestamp(packet.time);

    lock_guard<mutex> lock(g_flightsMutex);

    auto it = g_flights.find(packet.planeId);
    if (it == g_flights.end())  return;

    FlightRecord& rec = it->second;

    if (!rec.hasFirstPacket)
    {
        rec.hasFirstPacket = true;
        rec.firstTimestamp = timestamp;
        rec.lastTimestamp = timestamp;
        rec.initialFuel = packet.fuel;// SRV-FUN-003: store initial fuel
        rec.currentFuel = packet.fuel;
        rec.previousTimestamp = timestamp;
        rec.previousFuel = packet.fuel;
        rec.elapsedSeconds = 0;
        rec.runningAverageConsumption = 0.0;// SRV-FUN-004: initialise avg
        return;
    }

    // SUBSEQUENT PACKETS
    int deltaSeconds = parseElapsedSeconds(rec.previousTimestamp, timestamp);
    double fuelConsumedSinceLast = rec.previousFuel - packet.fuel;// SRV-FUN-003b: Parse remaining fuel — already decoded from the binary

    // Fuel should never increase mid-flight
    if (fuelConsumedSinceLast < 0.0)    fuelConsumedSinceLast = 0.0;
    

    // SRV-FUN-004a: Accumulate elapsed time and fuel consumed.
    rec.elapsedSeconds += deltaSeconds;
    rec.cumulativeFuelConsumed += fuelConsumedSinceLast;
    // SRV-FUN-004b: Update the live fuel level shown in the display table.
    rec.currentFuel = packet.fuel;
    rec.lastTimestamp = timestamp;
    // SRV-FUN-004c: Recalculate running average consumption (kg/s).
    if (rec.elapsedSeconds > 0)   
        rec.runningAverageConsumption = rec.cumulativeFuelConsumed / rec.elapsedSeconds;
    
    // Advance the sliding window for the next packet comparison.
    rec.previousTimestamp = timestamp;
    rec.previousFuel = packet.fuel;
}


void clientHandler(SOCKET clientSocket, sockaddr_in clientAddr)
{
    char ipBuffer[INET_ADDRSTRLEN] = {};
    inet_ntop(AF_INET, &(clientAddr.sin_addr), ipBuffer, INET_ADDRSTRLEN);
    string clientIp = ipBuffer;

    
    int planeId = g_nextPlaneId.fetch_add(1);

    logMessage("Client connected from " + clientIp +
        " | Assigned Plane ID = " + to_string(planeId), LogLevel::INFO);
    initializeFlightRecord(planeId, clientIp);

    int assignedIdNetworkOrder = htonl(planeId);
    if (!sendAll(clientSocket, reinterpret_cast<const char*>(&assignedIdNetworkOrder), sizeof(assignedIdNetworkOrder)))
    {
        logMessage("ERROR: Failed to send assigned ID to client " + clientIp);
        finalizeFlight(planeId, FlightStatus::DISCONNECTED);
        closesocket(clientSocket);
        return;
    }

    int malformedCount = 0;
    constexpr int MAX_MALFORMED = 10; // kick client after 10 consecutive bad packets

    while (true)
    {
        TelemetryPacket packet{};
        bool ok = recvAll(clientSocket, reinterpret_cast<char*>(&packet), sizeof(packet));

        if (!ok)
        {
            logMessage("Client disconnected unexpectedly | Plane ID = " +
                to_string(planeId) + " | IP = " + clientIp, LogLevel::WARNING);
            finalizeFlight(planeId, FlightStatus::DISCONNECTED);
            break;
        }

        string validationError = validatePacket(packet, planeId);
        if (!validationError.empty())
        {
            ++malformedCount;
            logMessage("Malformed packet from Plane ID " + to_string(planeId) +
                " (" + to_string(malformedCount) + "/" + to_string(MAX_MALFORMED) +
                "): " + validationError, LogLevel::WARNING);

            // If a client sends too many bad packets in a row, mark it and
            // disconnect it so it doesn't clog the server. (SRV-FUN-007)
            if (malformedCount >= MAX_MALFORMED)
            {
                logMessage("Plane ID " + to_string(planeId) +
                    " exceeded malformed packet limit. Closing connection.", LogLevel::ERR);
                finalizeFlight(planeId, FlightStatus::MALFORMED);
                break;
            }
            continue;
        }

        // Reset malformed counter on a good packet
        malformedCount = 0;
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
        if (!isValidIPv4(bindIp))
        {
            cout << "[ERROR] Invalid bind IP address: \"" << bindIp << "\"\n"
                << "Usage: server.exe [bind_ip] [port]\n"
                << "  bind_ip  - IPv4 address to listen on (default: 0.0.0.0)\n"
                << "  port     - port number 1-65535       (default: " << DEFAULT_PORT << ")\n";
            return 1;
        }
    }

    if (argc >= 3)
    {
        
        int rawPort = atoi(argv[2]);
        if (!isValidPort(rawPort))
        {
            cout << "[ERROR] Invalid port number: \"" << argv[2] << "\"\n"
                << "Port must be between 1 and 65535.\n";
            return 1;
        }
        port = rawPort;
    }

	//Ctrl +C handler for graceful shutdown
    if (!SetConsoleCtrlHandler(consoleCtrlHandler, TRUE))
    {
        cout << "[WARN] Could not register console control handler. "
            "Use Ctrl+C to stop (may not shut down cleanly).\n";
    }

    thread printThread([] { g_printQueue.drainLoop(); });


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

    g_listenSocket = listenSocket;
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

    while (g_serverRunning)
    {
        sockaddr_in clientAddr{};
        int clientAddrLen = sizeof(clientAddr);

        SOCKET clientSocket = accept(listenSocket, reinterpret_cast<sockaddr*>(&clientAddr), &clientAddrLen);
        if (clientSocket == INVALID_SOCKET)
        {
            //logMessage("ERROR: accept() failed, continuing...");
            if (!g_serverRunning) break;
            logMessage("accept() failed, continuing to listen...", LogLevel::WARNING);
            continue;
        }

        thread handler(clientHandler, clientSocket, clientAddr);
        handler.detach();
    }

    // Graceful shutdown: wait for display thread, then clean up Winsock.
    logMessage("Server shutting down. Waiting for display thread...");
    g_serverRunning = false;
    if (displayThread.joinable())
        displayThread.join();
    // Print one final table so the operator sees the end state of all flights.
    printFlightTable();
    logMessage("Final flight table printed. Goodbye.");

    g_printQueue.shutdown();
    if (printThread.joinable())
        printThread.join();

    if (g_listenSocket != INVALID_SOCKET)
        closesocket(g_listenSocket);
    WSACleanup();
    return 0;
}