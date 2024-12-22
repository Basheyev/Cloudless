/******************************************************************************
*
*  RecordFileIO class tests implementation
*
*  (C) Bolat Basheyev 2022
*
******************************************************************************/

#include "RecordFileIO.h"
#include "RecordFileIOTest.h"

#include <iostream>
#include <sstream>
#include <filesystem>


using namespace Cloudless;
using namespace Cloudless::Storage;


/*
*  @brief Generate data records in file
*  @param[in] filename - path to file
*  @param[in] recordsCount - total records to generate
*/
bool RecordFileIOTest::generateData(const char* filename, size_t recordsCount) {

	CachedFileIO cachedFile;
	char buffer[1024] = { 0 };

	if (!cachedFile.open(filename)) {
		std::cout << "ERROR: Can't open file '" << filename << "' in write mode.\n";
		return false;
	}

	// Wrapper
	RecordFileIO storage(cachedFile);
	std::cout << "[TEST] Generating " << recordsCount << " data records...";

	auto startTime = std::chrono::high_resolution_clock::now();
	uint32_t length;
	
	
	uint32_t randomNumber;	
	uint32_t padding = 16;
	for (size_t i = 0; i < recordsCount; i++) {
		std::stringstream ss;
		randomNumber = std::rand();
		ss << "This is record data #" << i << " and random number " << randomNumber;
		if (randomNumber % 2) ss << " with optional length of this string";
		length = (uint32_t)ss.str().length();
		memset(buffer, 0, sizeof(buffer));
		memcpy(buffer, ss.str().c_str(), length);
		storage.createRecord(buffer, length + padding);		
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	std::cout << "OK in " << (endTime - startTime).count() / 1000000000.0 << "s";
	//std::cout << " - " << cachedFile.getStats(CachedFileStats::WRITE_THROUGHPUT) << "Mb/s\n";

	return true;
}



bool RecordFileIOTest::readAscending(const char* filename, bool verbose) {
	CachedFileIO cachedFile;
	if (!cachedFile.open(filename, 16 * 1024 * 1024)) {
		std::cout << "ERROR: Can't open file '" << filename << "' in write mode.\n";
		return false;
	}

	RecordFileIO db(cachedFile);


	std::cout << "[TEST] Reading " << db.getTotalRecords() << " data records in ASCENDING order...\n";
	if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	auto startTime = std::chrono::high_resolution_clock::now();
	db.first();
	size_t counter = 0;
	size_t prev, next;
	char* buffer = new char[65536];
	do {
		uint32_t length = db.getDataLength();
		prev = db.getPrevPosition();
		next = db.getNextPosition();		
		if (db.getRecordData(buffer, length) == NOT_FOUND) break;
		buffer[length] = 0;
		if (verbose) {
			std::cout << "Record at position: " << db.getPosition();
			std::cout << " Previous: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << db.getDataLength();
			std::cout << "\n";
			std::cout << "Data: '" << buffer << "'\n\n";
		}
		counter++;
	} while (db.next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	delete[] buffer;	
	std::cout << "TOTAL READ: " << counter << " records ";
	std::cout << "in " << (endTime - startTime).count() / 1000000000.0 << "s";
	//std::cout << " - " << cachedFile.getStats(CachedFileStats::READ_THROUGHPUT) << "Mb/s";
	std::cout << " - [" << ((db.getTotalRecords() == counter) ? "OK]\n" : "FAILED!]\n");
	return true;
}



bool RecordFileIOTest::readDescending(const char* filename, bool verbose) {
	CachedFileIO cachedFile;
	if (!cachedFile.open(filename)) {
		std::cout << "ERROR: Can't open file '" << filename << "' in write mode.\n";
		return false;
	}
	RecordFileIO db(cachedFile);
	std::cout << "[TEST] Reading " << db.getTotalRecords() << " data records in DESCENDING order...\n";
	if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	db.last();
	size_t counter = 0;
	size_t prev, next;
	char* buffer = new char[65536];
	do {
		uint32_t length = db.getDataLength();
		prev = db.getPrevPosition();
		next = db.getNextPosition();					
		if (db.getRecordData(buffer, length)==NOT_FOUND) break;
		buffer[length] = 0;
		if (verbose) {
			std::cout << "Record at position: " << db.getPosition();
			std::cout << " Previous: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << db.getDataLength();
			std::cout << "\n";
			std::cout << "Data: '" << buffer << "'\n\n";
		}
		counter++;
	} while (db.previous());
	delete[] buffer;
	std::cout << "TOTAL READ: " << counter << " records\n\n";
	return true;
}


bool RecordFileIOTest::removeEvenRecords(const char* filename, bool verbose) {
	CachedFileIO cachedFile;
	if (!cachedFile.open(filename)) {
		std::cout << "ERROR: Can't open file '" << filename << "' in write mode.\n";
		return false;
	}
	RecordFileIO db(cachedFile);
	std::cout << "[TEST] Deleting even data records...\n";
	if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	auto startTime = std::chrono::high_resolution_clock::now();
	db.first();
	size_t counter = 0;
	size_t prev, next;
	do {
		uint32_t length = db.getDataLength();
		prev = db.getPrevPosition();
		next = db.getNextPosition();
		if (verbose) {
			std::cout << "Pos: " << db.getPosition();
			std::cout << " Prev: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << db.getDataLength();
			std::cout << " - DELETED \n";
		}
		db.removeRecord();
		counter++;
	} while (db.next() && db.next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	std::cout << "TOTAL DELETED: " << counter << " records ";
	std::cout << "in " << (endTime - startTime).count() / 1000000000.0 << "s";
	//std::cout << " - " << cachedFile.getStats(CachedFileStats::WRITE_THROUGHPUT) << "Mb/s\n";
	return true;
}

bool RecordFileIOTest::insertNewRecords(const char* filename, size_t recordsCount) {
	CachedFileIO cachedFile;
	if (!cachedFile.open(filename)) {
		std::cout << "ERROR: Can't open file '" << filename << "' in write mode.\n";
		return false;
	}

	RecordFileIO storage(cachedFile);

	std::cout << "[TEST] Inserting " << recordsCount << " data records...";
	auto startTime = std::chrono::high_resolution_clock::now();
	uint32_t length;
	for (size_t i = 0; i < recordsCount; i++) {
		std::stringstream ss;
		ss << "inserted record data " << i*2 << " and " << std::rand();
		if (std::rand() % 2) ss << " suffix";
		length = (uint32_t)ss.str().length();
		std::string str = ss.str();
		const char* dataPtr = str.c_str();
		storage.createRecord(dataPtr, length);
	}
	auto endTime = std::chrono::high_resolution_clock::now();
	std::cout << "OK in " << (endTime - startTime).count() / 1000000000.0 << "s";
	//std::cout << " - " << cachedFile.getStats(CachedFileStats::WRITE_THROUGHPUT) << "Mb/s\n";
	return true;
}


void RecordFileIOTest::run(const char* filename) {
	std::filesystem::remove(filename);
	generateData(filename, 1000);
	readAscending(filename,true);
	removeEvenRecords(filename,true);
	readDescending(filename, true);
	insertNewRecords(filename, 3);
	readAscending(filename, true);
}


void RecordFileIOTest::runLoadTest(const char* filename, size_t amount) {
	std::filesystem::remove(filename);
	generateData(filename, amount);
	readAscending(filename, false);
	removeEvenRecords(filename, false);
	insertNewRecords(filename, amount / 2);
	readAscending(filename, false);
}




void RecordFileIOTest::execute() {
	const char* path = "d://test.bin";
	CachedFileIO cf;
	cf.open(path);
	RecordFileIO rf(cf);

	std::string msg = "My message for binary records storage";
	// Convert timestamp to local time structure



	for (uint64_t i = 0; i < 5; i++) {
		std::stringstream ss;
		auto timestamp = std::time(nullptr);
		std::tm* localTime = std::localtime(&timestamp);
		std::string rnd(6 - i, '#');
		ss << msg << " #" << i << " timestamp:" << std::put_time(localTime, "%Y-%m-%d %H:%M:%S") << rnd;
		std::string s = ss.str();
		uint32_t len = (uint32_t)s.size();
		rf.createRecord(s.c_str(), len);
	}

	std::shared_ptr<RecordCursor> rc = rf.getFirstRecord();

	//rc->next();
	rf.removeRecord(rc);


	if (rc == nullptr) return 0;

	int i = 0;
	char str[128];
	do {
		uint32_t len = rc->getDataLength();
		if (rc->getRecordData(str, len)) {
			str[len] = 0;
			std::cout << "Record: " << i << " Payload: " << str << "\n";
		}
		i++;
	} while (rc->next());

	cf.close();

	//std::filesystem::remove(path);

}