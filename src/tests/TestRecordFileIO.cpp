/******************************************************************************
*
*  RecordFileIO class tests implementation
*
*  (C) Bolat Basheyev 2022
*
******************************************************************************/

#include "RecordFileIO.h"
#include "TestRecordFileIO.h"



using namespace Cloudless;
using namespace Cloudless::Storage;
using namespace Cloudless::Tests;


std::string TestRecordFileIO::getName() const {
	return "RecordFileIO input, output, consistency and performance";
}

void TestRecordFileIO::init() {
	fileName = (char*)"records.bin";
	samplesCount = 10000;

	// Functional test
	if (std::filesystem::exists(fileName)) {
		std::filesystem::remove(fileName);
	}
	
	if (!cachedFile.open(fileName, false, 16 * 1024 * 1024)) {
		std::cout << "ERROR: Can't open file '" << fileName << "' in write mode.\n";
		return;
	}

	db = std::make_shared<RecordFileIO>(cachedFile);

}

void TestRecordFileIO::execute() {
		
	generateData(samplesCount);
	readAscending(false);
	removeEvenRecords(false);
	readDescending(false);
	insertNewRecords(samplesCount / 2);
	readAscending(false);


	// multithreaded
	multithreaded();
}

bool TestRecordFileIO::verify() const {
	return true;
}


void TestRecordFileIO::cleanup() {

	db.reset();
	cachedFile.close();

}



//------------------------------------------------------------------------------------------------------------------



bool TestRecordFileIO::multithreaded() {
	
	// Break samples to hardware cores count
	uint64_t batchesCount = std::thread::hardware_concurrency() / 4;
	uint64_t batchSize = samplesCount / batchesCount;
	

	auto startTime = std::chrono::high_resolution_clock::now();
	{		
		std::vector<std::jthread> workers(batchesCount);
		for (uint64_t i = 0; i < batchesCount; i++) {
			workers.emplace_back([this, i, batchSize]() {
				{
					std::unique_lock lock(outputLock);
					std::cout << "Thread #" << i << " started\n";
				}
				generateData(batchSize);
				readAscending(false);
				removeEvenRecords(false);
				readDescending(false);
				insertNewRecords(batchSize / 2);
			});
		}
	}


	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double cachedDuration = duration.count() / 1000000000.0;
	//double throughput = (bytesWritten / (1024.0 * 1024.0)) / (cachedDuration / 1000.0);

	bool result = true;
	std::stringstream ss;
	ss << "Concurrent tests " << cachedDuration << " s \n";
	{
		std::unique_lock lock(outputLock);
		std::cout << ss.str();
	}

	return true;
}


/*
*  @brief Generate data records in file
*  @param[in] filename - path to file
*  @param[in] recordsCount - total records to generate
*/
bool TestRecordFileIO::generateData(size_t recordsCount) {

	
	char buffer[1024] = { 0 };


	{
		std::unique_lock lock(outputLock);
		std::cout << "[TEST] Generating " << recordsCount << " data records...\n";
	}

	auto startTime = std::chrono::high_resolution_clock::now();
	size_t bytesWritten = 0;
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
		db->createRecord(buffer, length + padding);		
		bytesWritten += length;
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesWritten / 1024.0 / 1024.0) / duration;

	{
		std::unique_lock lock(outputLock);
		std::cout << "OK in " << duration << "s payload throughput " << throughput << " Mb/s\n";
	}

	return true;
}



bool TestRecordFileIO::readAscending(bool verbose) {

	
	{
		std::unique_lock lock(outputLock);
		std::cout << "[TEST] Reading " << db->getTotalRecords() << " data records in ASCENDING order...\n";
		if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	}
	auto startTime = std::chrono::high_resolution_clock::now();

	auto cursor = db->getFirstRecord();
	
	size_t counter = 0;
	size_t prev, next;
	size_t bytesRead = 0;
	char* buffer = new char[65536];
	do {
		if (cursor != nullptr && !cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();		
		if (!cursor->getRecordData(buffer, length)) break;
		buffer[length] = 0;
		bytesRead += length;
		if (verbose) {			
			std::unique_lock lock(outputLock);
			std::cout << "Record at position: " << cursor->getPosition();
			std::cout << " Previous: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << cursor->getDataLength();
			std::cout << "\n";
			std::cout << "Data: '" << buffer << "'\n\n";
		}
		counter++;
	} while (cursor->next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesRead / 1024.0 / 1024.0) / duration;
	delete[] buffer;	
	{
		std::unique_lock lock(outputLock);
		std::cout << "TOTAL READ: " << counter << " records ";
		std::cout << "in " << duration << "s";
		std::cout << " payload throughput " << throughput << " Mb/s";
		std::cout << " - [" << ((db->getTotalRecords() == counter) ? "OK]\n" : "FAILED!]\n");
	}
	return true;
}



bool TestRecordFileIO::readDescending(bool verbose) {
	{
		std::unique_lock lock(outputLock);
		std::cout << "[TEST] Reading " << db->getTotalRecords() << " data records in DESCENDING order...\n";
		if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	}
	auto cursor = db->getLastRecord();
	size_t counter = 0;
	size_t prev, next;
	char* buffer = new char[65536];
	do {
		if (cursor != nullptr && !cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();
		if (!cursor->getRecordData(buffer, length)) break;
		buffer[length] = 0;
		if (verbose) {			
			std::unique_lock lock(outputLock);
			std::cout << "Record at position: " << cursor->getPosition();
			std::cout << " Previous: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << cursor->getDataLength();
			std::cout << "\n";
			std::cout << "Data: '" << buffer << "'\n\n";
		}
		counter++;
	} while (cursor->previous());
	delete[] buffer;
	{
		std::unique_lock lock(outputLock);
		std::cout << "TOTAL READ: " << counter << " records\n\n";
	}
	return true;
}


bool TestRecordFileIO::removeEvenRecords(bool verbose) {

	{
		std::unique_lock lock(outputLock);
		std::cout << "[TEST] Deleting even data records...\n";
		if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	}
	auto startTime = std::chrono::high_resolution_clock::now();
	auto cursor = db->getFirstRecord();
	size_t counter = 0;
	size_t prev, next;
	do {
		if (cursor != nullptr && !cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();
		if (verbose) {
			std::unique_lock lock(outputLock);
			std::cout << "Pos: " << cursor->getPosition();
			std::cout << " Prev: " << ((prev == NOT_FOUND) ? 0 : prev);
			std::cout << " Next: " << ((next == NOT_FOUND) ? 0 : next);
			std::cout << " Length: " << cursor->getDataLength();
			std::cout << " - DELETED \n";
		}
		db->removeRecord(cursor);
		counter++;		
	} while (cursor->next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	auto duration = (endTime - startTime).count() / 1000000000.0;

	{
		std::unique_lock lock(outputLock);
		std::cout << "TOTAL DELETED: " << counter << " records ";
		std::cout << "in " << duration << "s";
	}

	return true;
}

bool TestRecordFileIO::insertNewRecords(size_t recordsCount) {

	{
		std::unique_lock lock(outputLock);
		std::cout << "[TEST] Inserting " << recordsCount << " data records...\n";
	}
	auto startTime = std::chrono::high_resolution_clock::now();
	uint32_t length;
	for (size_t i = 0; i < recordsCount; i++) {
		std::stringstream ss;
		ss << "inserted record data " << i*2 << " and " << std::rand();
		if (std::rand() % 2) ss << " suffix";
		length = (uint32_t)ss.str().length();
		std::string str = ss.str();
		const char* dataPtr = str.c_str();
		db->createRecord(dataPtr, length);
	}
	auto endTime = std::chrono::high_resolution_clock::now();
	{
		std::unique_lock lock(outputLock);
		std::cout << "OK in " << (endTime - startTime).count() / 1000000000.0 << "s";
	}
	
	return true;
}



/*
void TestRecordFileIO::execute() {
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

}*/