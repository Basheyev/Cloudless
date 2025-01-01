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
	samplesCount = 1000;

	// Functional test
	if (std::filesystem::exists(fileName)) {
		std::filesystem::remove(fileName);
	}
	
	if (!cachedFile.open(fileName, false)) {
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

	multithreaded();

	std::stringstream ss;
	ss << "Total records: " << db->getTotalRecords();

	printResult(ss.str().c_str(), true);

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
	uint64_t batchesCount = std::thread::hardware_concurrency();
	uint64_t batchSize = samplesCount / batchesCount;
	

	auto startTime = std::chrono::high_resolution_clock::now();
	{		
		std::vector<std::jthread> workers(batchesCount);
		for (uint64_t i = 0; i < batchesCount; i++) {
			workers.emplace_back([this, i, batchSize]() {
				{
					std::unique_lock lock(outputLock);
					std::cout << "\tStarting thread #" << i << " started\n";
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
		printResult(ss.str().c_str(), result);
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
		if (db->createRecord(buffer, length + padding) != nullptr) {
			bytesWritten += length;
		}
	}

	db->flush();

	auto endTime = std::chrono::high_resolution_clock::now();
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesWritten / 1024.0 / 1024.0) / duration;

	bool result = (bytesWritten == length * recordsCount);
	std::stringstream ss;
	ss << "Generating " << recordsCount << " data records: " << duration << "s payload throughput " << throughput << " Mb/s";
	printResult(ss.str().c_str(), result);
	

	return true;
}



bool TestRecordFileIO::readAscending(bool verbose) {



	if (verbose) std::cout << "-----------------------------------------------------------\n\n";

	auto startTime = std::chrono::high_resolution_clock::now();

	auto cursor = db->getFirstRecord();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	size_t bytesRead = 0;
	char* buffer = new char[65536];
	do {
		if (!cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();		
		if (!cursor->getRecordData(buffer, length)) {
			std::unique_lock lock(outputLock);
			std::cout << "Record corrupt at " << counter << " record\n";
			break;
		}
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
		// TODO: check data corrpution and cycles references in file records

	} while (cursor->next() /* && counter <= samplesCount*/);
	auto endTime = std::chrono::high_resolution_clock::now();	
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesRead / 1024.0 / 1024.0) / duration;
	delete[] buffer;	
	{
		bool result = (counter == db->getTotalRecords());
		std::stringstream ss;		
		ss << "Reading " << counter << "/" << db->getTotalRecords() << " records in ASCENDING order ";
		ss << "in " << duration << "s";
		ss << " payload throughput " << throughput << " Mb/s";
		printResult(ss.str().c_str(), result);
	}
	return true;
}



bool TestRecordFileIO::readDescending(bool verbose) {


	if (verbose) std::cout << "-----------------------------------------------------------\n\n";

	auto cursor = db->getLastRecord();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	char* buffer = new char[65536];
	bool result = true;

	std::unordered_map<uint64_t, uint64_t> IDs;

	do {
		uint64_t pos = cursor->getPosition();
		if (!cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();
		if (!cursor->getRecordData(buffer, length)) {
			result = false;
			break;
		}
		else {
			if (IDs.find(pos) == IDs.end()) {
				IDs[pos] = counter;
			}
			else {
				std::cerr << "\n\nDUPLICATES IN TRAVERSAL!!!\n" << "Record ID=" << IDs[pos] << " repeats at=" << counter << "\n";
				break;
			}
		}
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
		// some how this exceeds records count but still in the loop!?
		counter++;
	} while (cursor->previous() /* && counter <= samplesCount*/);

	delete[] buffer;
	{
		size_t totalRecords = db->getTotalRecords();
		if (result) result = (counter == totalRecords);
		std::stringstream ss;
		ss << "Reading " << counter << "/" << db->getTotalRecords() << " records in DESCENDING order " << (counter > samplesCount ? " Cursor invalidated " : "");
		printResult(ss.str().c_str(), result);
	}
	return true;
}


bool TestRecordFileIO::removeEvenRecords(bool verbose) {

	{
		std::unique_lock lock(outputLock);
		
		if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	}
	auto startTime = std::chrono::high_resolution_clock::now();
	
	auto cursor = db->getFirstRecord();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	size_t amount = db->getTotalRecords();
	do {
		if (!cursor->isValid()) {
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
	} while (cursor->next() /* && counter < samplesCount*/);
	auto endTime = std::chrono::high_resolution_clock::now();	
	auto duration = (endTime - startTime).count() / 1000000000.0;

	{
		bool result = (counter == amount);
		std::stringstream ss;
		ss << "Deleting even data records. ";
		ss << "TOTAL DELETED: " << counter << "/" << samplesCount << " records ";
		ss << "in " << duration << "s";
		printResult(ss.str().c_str(), result);
	}

	return true;
}


bool TestRecordFileIO::insertNewRecords(size_t recordsCount) {

	
	auto startTime = std::chrono::high_resolution_clock::now();
	uint32_t length;
	bool result = true;
	for (size_t i = 0; i < recordsCount; i++) {
		std::stringstream ss;
		ss << "inserted record data " << i*2 << " and " << std::rand();
		if (std::rand() % 2) ss << " suffix";
		length = (uint32_t)ss.str().length();
		std::string str = ss.str();
		const char* dataPtr = str.c_str();
		result = result && (db->createRecord(dataPtr, length) != nullptr);
	}
	db->flush();
	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = (endTime - startTime).count() / 1000000000.0;
	{
		
		std::stringstream ss;
		ss << "Inserting " << recordsCount << " data records ";
		ss << "in " << duration << "s";
		printResult(ss.str().c_str(), result);
	}
	
	return true;
}
