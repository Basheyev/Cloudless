/******************************************************************************
*
*  CachedFileIO class tests implementation
*
*  (C) Bolat Basheyev 2022-2024
*
******************************************************************************/

#include "TestCachedFileIO.h"


using namespace Cloudless;
using namespace Cloudless::Storage;
using namespace Cloudless::Tests;



std::string TestCachedFileIO::getName() const {
	return "CachedFileIO input, output, consistency and performance";
}


void TestCachedFileIO::init() {
	// initialize parameters
	fileName = (char*) "cachedfile.bin";
	this->samplesCount = 1000000;
	this->docSize = 479;
	this->cacheRatio = 0.15;
	this->sigma = 0.04;
	// delete file if exists
	if (std::filesystem::exists(this->fileName)) {
		std::filesystem::remove(this->fileName);
	}
}


void TestCachedFileIO::execute() {	
	const char* message1 = "This is an initial message!";
	const char* message2 = "This is different message to overwrite!";
	const uint64_t messageLength = strlen(message1);
	const long long cycles = 1000000;

	testfileOpen(true);
	testReverseWrites(cycles, message1);
	testSequentialReads(cycles, message1);
	testIOAfterClose();
	testfileOpen(false);
	testSequentialWrites(cycles, message2);
	testSequentialReads(cycles, message2);
	testFileSize(cycles * strlen(message2));
	testRandomMultithreadWrites();
	double cachedThroughput = testRandomMultithreadReads();
	double stdioThroughput = stdioRandomReads();

	bool result = cachedThroughput > stdioThroughput;
	std::stringstream ss;
	ss << "CachedFileIO performance comparing to STDIO is " << (cachedThroughput / stdioThroughput * 100) << "%";
	printResult(ss.str().c_str(), result);
	cf.close();

}



bool TestCachedFileIO::verify() const {
	return true;
}


void TestCachedFileIO::cleanup() {
	if (std::filesystem::exists(this->fileName)) {
		std::filesystem::remove(this->fileName);
	}
}


//--------------------------------------------------------------------------------------------------------------


void TestCachedFileIO::testfileOpen(bool fullCheck) {
	bool result;	
	if (fullCheck) {
		result = !cf.open(nullptr);
		printResult("Call open(nullptr)", result);
		result = !cf.open("", true);
		printResult("Call open(\"\", true) in read only mode", result);
		result = !cf.open(fileName, true);
		printResult("Call open(\"\", true) in write mode", result);
		result = !cf.open("", true);
		printResult("Call open(\"file_not_found\", true) in read only mode", result);
	}
	result = cf.open(fileName);
	printResult("Call open(\"valid_file\") in random access mode", result);
}



void TestCachedFileIO::testSequentialWrites(long long cycles, const char* message) {
	size_t messageLength = strlen(message);
	bool writeFailed = false;
	for (long long i = 0; i < cycles; i++) {
		writeFailed = !cf.write(i * messageLength, message, messageLength);
		if (writeFailed) {
			break;
		}
	}
	bool result = !writeFailed || (!cf.flush());
	std::stringstream ss;
	ss << "Multiple sequential overwrites of " << cycles << " new messages";
	printResult(ss.str().c_str(), result);

}


void TestCachedFileIO::testReverseWrites(long long cycles, const char* message) {
	size_t messageLength = strlen(message);
	bool writeFailed = false;
	for (long long i = cycles - 1; i >= 0; --i) {
		writeFailed = !cf.write(i * messageLength, message, messageLength);
		if (writeFailed) break;
	}
	bool result = !writeFailed && cf.flush();
	std::stringstream ss;
	ss << "Multiple reverse writes of " << cycles << " messages";
	printResult(ss.str().c_str(), result);
}



/**
*
*  @brief Generates file with data
*  @return sequential write throughput in Mb/s
*
*/
double TestCachedFileIO::testRandomMultithreadWrites() {

	CachedFileIO cachedFile;

	char buf[256] =
		"\n{\n\t\"name:\": \"unknown\",\n\t\"birthDate\": \"unknown\",\n\t"
		"\"GUID\" : \"6B29FC40-CA47-1067-B31D-00DD010662DA\",\n\t"
		"\"letters\": ['a','b','c','d','e','f','g'],\n\t\"id\": ";


	size_t bytesWritten = 0;
	size_t length = strlen(buf);
	size_t pos = 0;

	size_t fileSize = cf.getFileSize();
	cf.setCacheSize(size_t(fileSize * cacheRatio));

	// Break samples to hardware cores count
	uint64_t batchesCount = std::thread::hardware_concurrency();
	uint64_t batchSize = samplesCount / batchesCount;

	cf.resetStats();

	auto startTime = std::chrono::high_resolution_clock::now();
	{
		std::vector<std::jthread> workers(batchesCount);
		for (uint64_t i = 0; i < batchesCount; i++) {
			workers.emplace_back([this, i, batchSize, buf]() {
				this->testRandomWritesThread(i, batchSize, buf);
				});
			bytesWritten += batchSize * length;
		}
	}

	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double cachedDuration = duration.count() / 1000000.0;
	double throughput = (bytesWritten / (1024.0 * 1024.0)) / (cachedDuration / 1000.0);

	bool result = true;
	std::stringstream ss;
	ss << "Concurrent writes throughput " << throughput << " Mb/s ";
	ss << "(cache hits rate " << cf.getStats(CachedFileStats::CACHE_HITS_RATE) << "%)";
	printResult(ss.str().c_str(), result);

	return throughput;
}


void TestCachedFileIO::testRandomWritesThread(uint64_t batchNo, uint64_t cycles, const char* msg) {

	const size_t length = strlen(msg);

	size_t fileSize = cf.getFileSize();
	size_t bytesWritten = 0;
	size_t offset;

	bool result = true;

	for (size_t i = 0; i < cycles; i++) {
		// generate random
		offset = size_t(randNormal(0.5, this->sigma) * double(fileSize - length));
		
		if (offset < fileSize) {
			if (!cf.write(offset, msg, length)) {
				result = false;
				break;
			}
			bytesWritten += length;
		}
	}

	std::stringstream ss;
	ss << "Thread #" << batchNo << " - random writes of " << cycles << " messages ( " << bytesWritten << " bytes)";
	printResult(ss.str().c_str(), result);

}


void TestCachedFileIO::testSequentialReads(long long cycles, const char* message) {
	size_t messageLength = strlen(message);	
	{
		char* strbuf = new char[messageLength + 1];
		bool readFailed = false;
		for (long long i = 0; i < cycles; i++) {
			readFailed = !cf.read(i * messageLength, strbuf, messageLength);
			if (readFailed) {
				break;
			}
			strbuf[messageLength] = 0; // string null terminator
			if (strcmp(message, strbuf) != 0) {
				readFailed = true;
				break;
			}
		}
		bool result = !readFailed;
		std::stringstream ss;
		ss << "Multiple sequential reads of " << cycles << " messages and comparing to original message";
		printResult(ss.str().c_str(), result);
		delete[] strbuf;
	}
}



/**
*
*  @brief Random reads using cache as 10% size of file
*  @return random read throughput in Mb/s
*
*/
double TestCachedFileIO::testRandomMultithreadReads() {

	CachedFileIO cachedFile;

	size_t bytesRead = 0;

	if (!cf.open(this->fileName)) return false;

	size_t fileSize = cf.getFileSize();
	cf.setCacheSize(size_t(fileSize * cacheRatio));

	size_t length = docSize;

	// Break samples to hardware cores count
	uint64_t batchesCount = std::thread::hardware_concurrency();
	uint64_t batchSize = samplesCount / batchesCount;

	cf.resetStats();

	auto startTime = std::chrono::high_resolution_clock::now();
	{
		std::vector<std::jthread> workers(10);
		for (uint64_t i = 0; i < batchesCount; i++) {
			workers.emplace_back([this, i, batchSize, length]() {
				this->testRandomReadsThread(i, batchSize, length);
				});
			bytesRead += batchSize * length;
		}

	}
	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double readTime = duration.count() / 1000000.0;
	double throughput = (bytesRead / (1024.0 * 1024.0)) / (readTime / 1000.0);

	

	cf.close();

	bool result = true;
	std::stringstream ss;	
	ss << "Concurrent random reads " << throughput << " Mb/sec, " 
	   << "(cache hit rate: " << cf.getStats(CachedFileStats::CACHE_HITS_RATE) << "%)";
	printResult(ss.str().c_str(), result);

	return throughput;
}



void TestCachedFileIO::testRandomReadsThread(uint64_t batchNo, uint64_t batchSize, uint64_t length) {

	char* buf = new char[PAGE_SIZE * 4];
	size_t fileSize = cf.getFileSize();
	size_t bytesRead = 0;
	size_t offset;

	for (size_t i = 0; i < batchSize; i++) {
		// generate random
		offset = size_t(randNormal(0.5, this->sigma) * double(fileSize - length));
		// offset always positive because its size_t
		if (offset < fileSize) {
			cf.read(offset, buf, length);
			buf[length + 1] = 0;
			bytesRead += length;
		}
	}

	delete[] buf;

	std::stringstream ss;
	bool result = true;
	ss << "Thread #" << batchNo << " - random reads of " << batchSize << " messages ( " << bytesRead << " bytes)";
	printResult(ss.str().c_str(), result);
}



/**
*
*  @brief Random reads using STDIO
*  @return random read throughput in Mb/s
*/
double TestCachedFileIO::stdioRandomReads() {

	FILE* file = nullptr;

	char* buf = new char[PAGE_SIZE * 4];
	size_t length, bytesWritten = 0;
	size_t offset;

	errno_t result = fopen_s(&file, this->fileName, "r+b");
	if (result != 0 || file == nullptr) return -1;

	size_t fileSize = std::filesystem::file_size(this->fileName);

	auto startTime = std::chrono::high_resolution_clock::now();
	length = docSize;

	for (size_t i = 0; i < samplesCount; i++) {

		offset = (size_t)(randNormal(0.5, sigma) * double(fileSize - length));

		if (offset < fileSize) {			
			_fseeki64(file, offset, SEEK_SET);
			fread(buf, 1, length, file);
			buf[length + 1] = 0;
			bytesWritten += length;
		}

	}
		
	fclose(file);
	delete[] buf;

	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double readTime = duration.count() / 1000000.0;
	double throughput = (bytesWritten / (1024.0 * 1024.0)) / (readTime / 1000.0);

	bool stdioResult = true;
	std::stringstream ss;
	ss << "STDIO random reads " << throughput << " Mb/sec";
	printResult(ss.str().c_str(), stdioResult);

	return throughput;
}



void TestCachedFileIO::testIOAfterClose() {

	bool result = cf.close();
	printResult("Call close() cached file", result);
	
	const char* msg = "Access should be denied!";
	const size_t msg_len = strlen(msg);

	result = !cf.write(0, msg, msg_len);
	printResult("Writes of short message after file closed should fail", result);
	
	char* strbuf = new char[msg_len + 1];
	result = !cf.read(0, strbuf, msg_len);
	delete[] strbuf;
	printResult("Read of short message after file closed", result);
	
}


void TestCachedFileIO::testFileSize(uint64_t expectedDataSize) {

	size_t fileSize = cf.getFileSize();
	size_t expectedPages = (expectedDataSize / PAGE_SIZE) + (expectedDataSize % PAGE_SIZE ? 1 : 0);
	size_t expectedFileSize = expectedPages * PAGE_SIZE;
	bool result = fileSize == expectedFileSize;
	printResult("Checking getFileSize()", result);

	size_t filesystemSize = std::filesystem::file_size(fileName);
	result = cf.getFileSize() == filesystemSize;
	printResult("Comparing getFileSize() to std::filesystem", result);
}



/**
*
*  @brief  Box Muller Method
*  @return normal distributed random number
*
*/
double TestCachedFileIO::randNormal(double mean, double stddev)
{
	static double n2 = 0.0;
	static int n2_cached = 0;
	if (!n2_cached) {
		double x, y, r;
		do {
			x = 2.0 * rand() / RAND_MAX - 1.0;
			y = 2.0 * rand() / RAND_MAX - 1.0;
			r = x * x + y * y;
		} while (r == 0.0 || r > 1.0);
		double d = sqrt(-2.0 * log(r) / r);
		double n1 = x * d;
		n2 = y * d;
		double result = n1 * stddev + mean;
		n2_cached = 1;
		return result;
	}
	else {
		n2_cached = 0;
		return n2 * stddev + mean;
	}
}



void TestCachedFileIO::printResult(const char* useCase, bool result) {
	if (useCase == nullptr) return;
	size_t length = strlen(useCase);
	std::stringstream ss;
	ss << "\t" << useCase;
	if (length < 90) {
		size_t blanksCount = 90 - length;
		std::string blanks(blanksCount, '.');	
		ss << blanks;
	}
	std::lock_guard lock(outputLock);
	std::cout << ss.str() << " " << (result ? "OK" : "FAILED") << "\n";
}





