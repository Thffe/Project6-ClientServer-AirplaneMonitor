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


//graceful SHUTDOWN
// Tracks the listening socket globally so the shutdown handler can close it,
// which causes accept() to fail and breaks the main accept loop cleanly.
// Addresses the missing graceful shutdown — previously the server could only
// be killed by force, risking incomplete log file writes.
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
const string LOG_FILE = "flight_records.txt";

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

//
//void logMessage(const string& msg)
//{
//    lock_guard<mutex> lock(g_consoleMutex);
//    cout << "[" << currentDateTimeString() << "] " << msg << endl;
//}
void logMessage(const string& msg, LogLevel level = LogLevel::INFO)
{
    string tag;
    switch (level)
    {
    case LogLevel::INFO:    tag = "[INFO]   "; break;
    case LogLevel::WARNING: tag = "[WARN]   "; break;
    case LogLevel::ERR:     tag = "[ERROR]  "; break;
    }

    lock_guard<mutex> lock(g_consoleMutex);
    cout << "[" << currentDateTimeString() << "] " << tag << msg << endl;
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
        if (!(isdigit((unsigned char)c) || c == '_' || c == ':' || c == ' '))
            return "timestamp contains invalid character '" + string(1, c) + "'";
        ++timeLen;
    }
    if (!foundNull)
        return "timestamp missing null terminator";
    if (timeLen < 5)
        return "timestamp too short (" + to_string(timeLen) + " chars)";

    // Validate fuel value
    if (!isfinite(pkt.fuel))
        return "fuel value is NaN or infinite";
    if (pkt.fuel < 0.0)
        return "fuel value is negative (" + to_string(pkt.fuel) + " kg)";
    if (pkt.fuel > 200000.0)
        return "fuel value implausibly large (" + to_string(pkt.fuel) + " kg)";

    return ""; // valid
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


//log file
//restarting the server appends correctly without a duplicate header row, and a brand-new file always gets a header.
bool logFileHasHeader()
{
    ifstream check(FINAL_LOG_FILE);
    if (!check.is_open()) return false;     // file doesn't exist yet
    string firstLine;
    getline(check, firstLine);
    return !firstLine.empty();              // has content = has header
}

//void appendFinalRecordToFile(const FlightRecord& record)
//{
//    lock_guard<mutex> lock(g_fileMutex);
//
//    static bool headerWritten = false;
//
//    ofstream out(FINAL_LOG_FILE, ios::app);
//    if (!out.is_open())
//    {
//        logMessage("ERROR: Could not open final flight log file.");
//        return;
//    }
//
//    if (!headerWritten)
//    {
//        out << "PlaneID,ClientIP,FirstTimestamp,LastTimestamp,ElapsedSeconds,InitialFuel,FinalFuel,FinalAverageConsumption,Status\n";
//        headerWritten = true;
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
//}
//

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
        " | Avg consumption: " +
        to_string(record.finalAverageConsumption) + " kg/s");
}


//// Added columns: Client IP, First Timestamp, Initial Fuel.
// All value columns now show units in the header (kg, s, kg/s) per SRV-USE-005.
// Active and completed flights are printed in separate sections so the operator
// can quickly distinguish live flights from finished ones (SRV-USE-001).

//void printFlightTable()
//{
//    lock_guard<mutex> consoleLock(g_consoleMutex);
//    lock_guard<mutex> dataLock(g_flightsMutex);
//
//    cout << "\n=====================================================================================\n";
//    cout << left
//        << setw(10) << "Plane ID"
//        << setw(18) << "Elapsed (s)"
//        << setw(18) << "Fuel (kg)"
//        << setw(22) << "Avg Cons. (kg/s)"
//        << setw(18) << "Status"
//        << "\n";
//    cout << "-------------------------------------------------------------------------------------\n";
//
//    for (const auto& [id, rec] : g_flights)
//    {
//        cout << left
//            << setw(10) << rec.planeId
//            << setw(18) << rec.elapsedSeconds
//            << setw(18) << fixed << setprecision(3) << rec.currentFuel
//            << setw(22) << fixed << setprecision(6) << rec.runningAverageConsumption
//            << setw(18) << statusToString(rec.status)
//            << "\n";
//    }
//
//    cout << "=====================================================================================\n\n";
//}

void printFlightTable()
{
    lock_guard<mutex> consoleLock(g_consoleMutex);
    lock_guard<mutex> dataLock(g_flightsMutex);

    ofstream out(LOG_FILE, ios::app);
    if (!out.is_open())
    {
        logMessage("Could not open flight log file: " + LOG_FILE, LogLevel::ERR);
        return;
    }

    out << "\n";
    out << "====================================================================================="
        "==========================\n";
    out << left
        << setw(10) << "PlaneID"
        << setw(18) << "Client IP"
        << setw(12) << "Time (s)"
        << setw(16) << "Init Fuel (kg)"
        << setw(16) << "Fuel Now (kg)"
        << setw(20) << "Avg Cons (kg/s)"
        << setw(14) << "Status"
        << "\n";
    out << "-------------------------------------------------------------------------------------"
        "--------------------------\n";

    bool hasActive = false;
    bool hasFinished = false;

    // Active flights first
    for (const auto& [id, rec] : g_flights)
    {
        if (rec.status != FlightStatus::ACTIVE) continue;
        hasActive = true;
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
    if (!hasActive)
        out << "  (no active flights)\n";

    out << "--- Completed / Disconnected "
        "--------------------------------------------------------------------------\n";

    // Finished flights below the divider
    for (const auto& [id, rec] : g_flights)
    {
        if (rec.status == FlightStatus::ACTIVE) continue;
        hasFinished = true;
        out << left
            << setw(10) << rec.planeId
            << setw(18) << rec.clientIp
            << setw(12) << rec.elapsedSeconds
            << setw(16) << fixed << setprecision(2) << rec.initialFuel
            << setw(16) << fixed << setprecision(2) << rec.finalFuel
            << setw(20) << fixed << setprecision(6) << rec.finalAverageConsumption
            << setw(14) << statusToString(rec.status)
            << "\n";
    }
    if (!hasFinished)
        out << "  (none yet)\n";

    out << "====================================================================================="
        "==========================\n\n";
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

        g_flights.erase(it);
    }

    appendFinalRecordToFile(finalCopy);
}

// ======================================================
// ALBERT SECTION START
// ======================================================

/*
 * updateFlightFromPacket
 *
 * Called by clientHandler() each time a valid TelemetryPacket is received.
 *
 * Responsibilities (SRV-FUN-002, SRV-FUN-003, SRV-FUN-004):
 *   - Read the packet's planeId, timestamp, and fuel fields
 *   - Parse the timestamp to compute elapsed seconds since the previous packet
 *   - Calculate instantaneous fuel consumed since last packet
 *   - Update the running average consumption: cumulativeFuelConsumed / elapsedSeconds
 *   - Write all results back into the shared FlightRecord (mutex already held)
 *
 * Fuel consumption formula (from SDD):
 *   instantaneous consumption = (prevFuel - currentFuel) / deltaSeconds
 *   running average           = cumulativeFuelConsumed  / totalElapsedSeconds
 *
 * Thread safety: g_flightsMutex is acquired inside this function for the
 * entire duration of the update so no other thread can corrupt the record.
 */
void updateFlightFromPacket(const TelemetryPacket& packet)
{
    // Build a std::string from the null-terminated char array in the packet.
    // The caller (clientHandler) has already validated that the time field
    // contains only printable, well-formed characters via isPrintableTimeString().
    string timestamp(packet.time);

    lock_guard<mutex> lock(g_flightsMutex);

    // Locate this plane's record in the shared data store.
    auto it = g_flights.find(packet.planeId);
    if (it == g_flights.end())  return;// Record was never initialised

    FlightRecord& rec = it->second;

    // FIRST PACKET for this flight
    // Establish the baseline values; no consumption can be calculated yet
    // because we have no previous data point to compare against.
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

    // SRV-FUN-003a: Parse timing: compute how many seconds have passed
    // since the previous packet using the timestamps embedded in the data.
    // parseElapsedSeconds() handles the custom "M_D_Y H:MM:SS" format used
    // by all four telemetry files and returns a minimum of 1 to avoid /0.
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
    {
        rec.runningAverageConsumption = rec.cumulativeFuelConsumed / rec.elapsedSeconds;
    }
    // Advance the sliding window for the next packet comparison.
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

    //logMessage("Client connected from " + clientIp + ". Assigned Plane ID = " + to_string(planeId));
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

    DWORD recvTimeout = 10000; // 10 seconds
    setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO,
        (const char*)&recvTimeout, sizeof(recvTimeout));

    int malformedCount = 0;
    constexpr int MAX_MALFORMED = 10;

    while (true)
    {
        TelemetryPacket packet{};
        bool ok = recvAll(clientSocket, reinterpret_cast<char*>(&packet), sizeof(packet));

        if (!ok)
        {
            // This now catches BOTH clean disconnects AND powered-off clients
            logMessage("Client disconnected | Plane ID = " +
                to_string(planeId) + " | IP = " + clientIp, LogLevel::WARNING);
            finalizeFlight(planeId, FlightStatus::DISCONNECTED);
            break;
        }


        // Returns a description string — empty string means packet is valid.
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

        //if (packet.planeId != planeId)
        //{
        //    logMessage("Malformed packet rejected: ID mismatch for Plane ID = " + to_string(planeId));
        //    continue;
        //}

        //if (!isPrintableTimeString(packet.time))
        //{
        //    logMessage("Malformed packet rejected: invalid timestamp for Plane ID = " + to_string(planeId));
        //    continue;
        //}

        //if (packet.fuel < 0.0)
        //{
        //    logMessage("Malformed packet rejected: negative fuel for Plane ID = " + to_string(planeId));
        //    continue;
        //}

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
        //port = atoi(argv[2]);
        //if (port <= 0)
        //{
        //    cout << "ERROR: Invalid port." << endl;
        //    return 1;
        //}
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

    if (g_listenSocket != INVALID_SOCKET)
        closesocket(g_listenSocket);
    WSACleanup();
    return 0;
}