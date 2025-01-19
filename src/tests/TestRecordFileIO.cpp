/******************************************************************************
*
*  RecordFileIO class tests implementation
*
*  (C) Bolat Basheyev 2022-2025
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
	
	db = std::make_shared<RecordFileIO>();

	if (!db->open(fileName, false, 24 * samplesCount)) {
		std::cout << "ERROR: Can't open file '" << fileName << "' in write mode.\n";
		return;
	}	

}

void TestRecordFileIO::execute() {
		

	singlethreaded();
	multithreaded();

	std::stringstream ss;
	ss << "Total records: " << db->getTotalRecords();

	printResult(ss.str().c_str(), true);

}

bool TestRecordFileIO::verify() const {
	return true;
}


void TestRecordFileIO::cleanup() {
	db->close();
	db.reset();
}



//------------------------------------------------------------------------------------------------------------------

bool TestRecordFileIO::singlethreaded() {
	auto startTime = std::chrono::high_resolution_clock::now();
	generateData(samplesCount);
	readAscending(false);
	removeEvenRecords(false);
	readDescending(false);
	insertNewRecords(samplesCount / 2);
	readAscending(false);
	editRecords(false);
	db->flush();
	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double durationInSeconds = duration.count() / 1000000000.0;
	
	bool result = true;
	std::stringstream ss;
	ss << "SINGLE THREAD test completed in  " << durationInSeconds << " s (Samples: " << samplesCount << ")";
	printResult(ss.str().c_str(), result);
	return result;
}


bool TestRecordFileIO::multithreaded() {
	
	// Break samples to hardware cores count
	uint64_t batchesCount = std::thread::hardware_concurrency();
	uint64_t batchSize = samplesCount / batchesCount;
	

	auto startTime = std::chrono::high_resolution_clock::now();
	{		
		std::vector<std::jthread> workers(batchesCount);
		for (uint64_t i = 0; i < batchesCount; i++) {
			workers.emplace_back([this, i, batchSize]() {		

				// every 3rd thread is writer
				if (i % 3 == 0) {
					generateData(batchSize);
					//editRecords(false);
					//readDescending(false);
				    //removeEvenRecords(false);
					//insertNewRecords(batchSize / 2);					
				} else {
					readAscending(false);					
					//readAscending(false);
				}				
			});
		}		

		std::stringstream ss;
		ss << "Total threads started: " << batchesCount;
		printResult(ss.str().c_str(), true);
		
	}

	//readAscending(true);


	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double durationInSeconds = duration.count() / 1000000000.0;
	//double throughput = (bytesWritten / (1024.0 * 1024.0)) / (cachedDuration / 1000.0);

	bool result = true;
	std::stringstream ss;
	ss << "MULTITHREAD test completed in  " << durationInSeconds << " s (Threads:" << batchesCount << ", Batch size: " << batchSize << ")";
	printResult(ss.str().c_str(), result);
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
	uint32_t length = 0;		
	uint32_t randomNumber;	
	uint32_t padding = 16;
	bool result = true;

	for (size_t i = 0; i < recordsCount; i++) {
		std::stringstream ss;
		randomNumber = std::rand();
		ss << "This is record data #" << i << " and random number " << randomNumber;
		if (randomNumber % 2) ss << " with optional length of this string";
		length = (uint32_t)ss.str().length();
		memset(buffer, 0, sizeof(buffer));
		memcpy(buffer, ss.str().c_str(), length);		
		if (db->createRecord(buffer, length + padding) != nullptr) {
			bytesWritten += (size_t) (length + padding);
		}
		else {
			result = false;
			break;
		}
	}

	db->flush();

	auto endTime = std::chrono::high_resolution_clock::now();
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesWritten / 1024.0 / 1024.0) / duration;

	std::stringstream ss;
	ss << "Generating " << recordsCount << " data records: " << duration << "s payload throughput " << throughput << " Mb/s";
	printResult(ss.str().c_str(), result);
	

	return true;
}



bool TestRecordFileIO::readAscending(bool verbose) {



	if (verbose) std::cout << "-----------------------------------------------------------\n\n";

	auto startTime = std::chrono::high_resolution_clock::now();

	auto cursor = db->getFirstRecord();
	auto totalRecs = db->getTotalRecords();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	size_t bytesRead = 0;
	char* buffer = new char[65536];
	bool result = true;

	do {
		if (!cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record (while reading ascending)\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();		
		if (!cursor->getRecordData(buffer, length)) {
			std::unique_lock lock(outputLock);
			std::cout << "Record corrupt at " << counter << " record (ascending)\n";
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
		uint64_t pos = cursor->getPosition();
		if (counter > (samplesCount * 3 / 2) && counter > db->getTotalRecords() || pos == prev) {
			std::unique_lock lock(outputLock);
			std::cerr << "\nREAD ASCENDING CYCLIC REFERENCE!!! counter " << counter << " is more than Total=" << db->getTotalRecords() << "\n";
			std::cerr << "Record=" << pos << " prev=" << prev << " next=" << next << "\n\n";
			result = false;
			break;
		}
	} while (cursor->next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesRead / 1024.0 / 1024.0) / duration;
	delete[] buffer;	
	{		
		std::stringstream ss;		
		ss << "Reading " << counter << "/" << totalRecs << " records in ASCENDING order.";
		ss << " Payload throughput " << throughput << " Mb/s";
		printResult(ss.str().c_str(), result);
	}
	return true;
}



bool TestRecordFileIO::readDescending(bool verbose) {


	if (verbose) std::cout << "-----------------------------------------------------------\n\n";

	auto cursor = db->getLastRecord();
	auto totalRecs = db->getTotalRecords();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	char* buffer = new char[65536];
	bool result = true;
	uint64_t bytesRead = 0;
	//std::unordered_map<uint64_t, uint64_t> IDs;

	auto startTime = std::chrono::high_resolution_clock::now();

	do {
		uint64_t pos = cursor->getPosition();
		if (!cursor->isValid()) {
			std::unique_lock lock(outputLock);
			std::cout << "Cursor invalidated at " << counter << " record while reading descending\n";
			break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();
		if (!cursor->getRecordData(buffer, length)) {
			std::unique_lock lock(outputLock);
			std::cout << "Record corrupt at " << counter << " record (descending)\n";
			result = false;
			break;
		}
		else {
			bytesRead += length;
			/*if (IDs.find(pos) == IDs.end()) {
				IDs[pos] = counter;
			}
			else {
				std::unique_lock lock(outputLock);
				std::cerr << "\n\nDUPLICATES IN TRAVERSAL!!!\n" << "Record ID=" << IDs[pos] << " repeats at=" << counter << "\n";
				break;
			}*/
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
		if (counter > (samplesCount * 3 / 2) && counter > db->getTotalRecords() || pos == prev) {
			std::unique_lock lock(outputLock);
			std::cerr << "\nREAD DESCENDING CYCLIC REFERENCE!!! counter " << counter << " is more than Total=" << db->getTotalRecords() << "\n";
			std::cerr << "Record=" << pos << " prev=" << prev << " next=" << next << "\n\n";
			result = false;
			break;
		}
	} while (cursor->previous());
	
	auto endTime = std::chrono::high_resolution_clock::now();
	double duration = (endTime - startTime).count() / 1000000000.0;

	delete[] buffer;
	{
		size_t totalRecords = db->getTotalRecords();		
		double throughput = (bytesRead / 1024.0 / 1024.0) / duration;
		std::stringstream ss;
		ss << "Reading " << counter << "/" << totalRecs << " records in DESCENDING order.";
		ss << " Payload throughput " << throughput << " Mb/s";
		printResult(ss.str().c_str(), result);
	}
	return true;
}


bool TestRecordFileIO::removeEvenRecords(bool verbose) {

	{
		std::unique_lock lock(outputLock);
		
		if (verbose) std::cout << "-----------------------------------------------------------\n\n";
	}

	std::unordered_map<uint64_t, uint64_t> IDs;


	auto startTime = std::chrono::high_resolution_clock::now();
	
	auto cursor = db->getFirstRecord();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	size_t amount = db->getTotalRecords();
	bool result = true;

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
		uint64_t pos = cursor->getPosition();
		db->removeRecord(cursor);
		/*if (db->removeRecord(cursor)) {
			if (IDs.find(pos) == IDs.end()) {
				IDs[pos] = counter;
			}
			else {
				std::unique_lock lock(outputLock);
				std::cerr << "\n\nDUPLICATES IN TRAVERSAL!!!\n" << "Record ID=" << IDs[pos] << " repeats at=" << counter << "\n";
				break;
			}
		}*/
		counter++;
		if (counter > (samplesCount * 3 / 2) && counter > db->getTotalRecords() || pos == prev) {
			std::unique_lock lock(outputLock);
			std::cerr << "\nDELETE CYCLIC REFERENCE!!! counter " << counter << " is more than Total=" << db->getTotalRecords() << "\n";
			std::cerr << "Record=" << pos << " prev=" << prev << " next=" << next << "\n\n";
			result = false;
			break;		
		}
	} while (cursor->next());
	auto endTime = std::chrono::high_resolution_clock::now();	
	auto duration = (endTime - startTime).count() / 1000000000.0;

	{		
		std::stringstream ss;
		ss << "Deleting even data records. ";
		ss << "TOTAL DELETED: " << counter << "/" << samplesCount << " records ";
		ss << "in " << duration << "s";
		printResult(ss.str().c_str(), result);
	}

	return result;
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
	
	return result;
}



bool TestRecordFileIO::editRecords(bool verbose) {


	if (verbose) std::cout << "-----------------------------------------------------------\n\n";

	auto startTime = std::chrono::high_resolution_clock::now();

	auto cursor = db->getFirstRecord();
	if (cursor == nullptr) return false;

	size_t counter = 0;
	size_t prev, next;
	size_t bytesRead = 0;
	char* buffer = new char[65536];
	bool result = true;

	do {
		if (!cursor->isValid()) {
			cursor = db->getFirstRecord();
			//break;
		}
		uint32_t length = cursor->getDataLength();
		prev = cursor->getPrevPosition();
		next = cursor->getNextPosition();

		if (!cursor->getRecordData(buffer, length)) {
			std::unique_lock lock(outputLock);
			std::cerr << "Record corrupt at " << counter << " record\n";
			break;
		}		
		buffer[length] = 0;


		std::stringstream entryStr;		
		entryStr << "EDITED Thread=" << std::this_thread::get_id();
		uint32_t entryStrLen = static_cast<uint32_t> (entryStr.str().length());
		if (!cursor->setRecordData(entryStr.str().c_str(), entryStrLen)) {
			std::unique_lock lock(outputLock);
			std::cerr << "\nRecord edit failed at " << cursor->getPosition() << "\n";
			result = false;
			break;
		}

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

		uint64_t pos = cursor->getPosition();

		if (counter > (samplesCount * 3 / 2) && counter > db->getTotalRecords() || pos == prev) {
			std::unique_lock lock(outputLock);
			std::cerr << "\nEDIT ASCENDING CYCLIC REFERENCE!!! counter " << counter << " is more than Total=" << db->getTotalRecords() << "\n";
			std::cerr << "Record=" << pos << " prev=" << prev << " next=" << next << "\n\n";
			result = false;
			break;
		}
	} while (cursor->next());
	auto endTime = std::chrono::high_resolution_clock::now();
	double duration = (endTime - startTime).count() / 1000000000.0;
	double throughput = (bytesRead / 1024.0 / 1024.0) / duration;
	delete[] buffer;
	{
		std::stringstream ss;
		ss << "Editing " << counter << "/" << db->getTotalRecords() << " records in ASCENDING order.";
		ss << " Payload throughput " << throughput << " Mb/s";
		printResult(ss.str().c_str(), result);
	}
	return result;	
}