/******************************************************************************
*
*  CachedFileIO class tests implementation
*
*  (C) Bolat Basheyev 2022-2024
*
******************************************************************************/

#include "CachedFileIOTest.h"


using namespace Cloudless;




CachedFileIOTest::CachedFileIOTest(const char* path) {
	this->fileName = path;
	this->samplesCount = 1000000;
	this->docSize = 384;
	this->cacheRatio = 0.1;
	this->sigma = 0.045;
}


CachedFileIOTest::~CachedFileIOTest() {

}


/**
* 
*  @brief Runs CachedFileIO tests and compares throughput to STDIO
*  @return true if CachedFileIO to STDIO performance ratio > 1, false otherwise
*/
bool CachedFileIOTest::run(size_t samples, size_t jsonSize, double cacheRatio, double sigma) {

	testBasicAPI();

	// initalize parameters
	/*this->samplesCount = samples;
	this->docSize = jsonSize;
	this->cacheRatio = cacheRatio;
	this->sigma = sigma;
	
	// thousands separator		
	std::cout << "[PARAMETERS] CachedFileIO test:" << std::endl;
	std::cout << "\tSamples count = " << samples << std::endl;
	std::cout << "\tJSON size = " << jsonSize << " bytes" << std::endl;
	std::cout << "\tCache page = " << PAGE_SIZE << " bytes" << std::endl;
	std::cout << "\tCache size = " << cacheRatio * 100 << "% of database size" << std::endl;;
	std::cout << "\tDistribution Sigma = " << sigma * 100.0;
	std::cout << "% (93.3% of requests localized in " << sigma * 100.0 * 6 << "% of database)\n\n";

	//double cachedPageWriteThroughput = cachedRandomPageWrites();

	std::this_thread::sleep_for(std::chrono::seconds(1));

	double cachedWriteThroughput = cachedRandomWrites();

	std::this_thread::sleep_for(std::chrono::seconds(1));

	double cachedThroughput = cachedRandomReads();

	std::this_thread::sleep_for(std::chrono::seconds(1));
	
	double stdioThroughput = stdioRandomReads();

	std::this_thread::sleep_for(std::chrono::seconds(1));
		
	double ratio = cachedThroughput / stdioThroughput; 

	std::cout << "[RESULT] Read throughput ratio in RANDOM OFFSET test (CACHED/STDIO): " << std::setprecision(4);
	if (ratio > 1.0) {
		std::cout << "+" << (ratio - 1.0) * 100 << "% - ";
		std::cout << "SUCCESS! :)\n";
	}
	else {
		std::cout << (ratio - 1.0) * 100 << "% - ";
		std::cout << "FAILED :(\n";
	}*/

	//return ratio > 1.0;
	return 0;
}



void CachedFileIOTest::printResult(const char* useCase, bool result) {
	if (useCase == nullptr) return;
	size_t length = strlen(useCase);
	size_t blanksCount = 80 - length;
	std::string blanks(blanksCount, '.');
	std::cout << "\t" << useCase << blanks << (result ? "OK" : "FAILED") << "\n";
}

/**
* 
*  @brief Test CachedFileIO methods on basic corner cases
* 
* 
*/
bool CachedFileIOTest::testBasicAPI() {

	const char* testName = "Basic API";
	const char* fileName = "cachedfile.db";	
	const char* message = "This is a text message!";					      
	const long long messageLength = strlen(message);
	const long long cycles = 1000000;
	bool result;

	CachedFileIO cf;
	//---------------------------------------------------------------------------------------------------
	std::cout << "[TEST] " << testName << ":" << std::endl;
	//---------------------------------------------------------------------------------------------------
	result = !cf.open(nullptr);
	printResult("Call open(nullptr)", result);	
	//---------------------------------------------------------------------------------------------------
	result = !cf.open("", true);	
	printResult("Call open(\"\", true) in read only mode", result);	
	//---------------------------------------------------------------------------------------------------
	result = !cf.open("file_not_found", true);
	printResult("Call open(\"file_not_found\", true) in read only mode", result);
	//---------------------------------------------------------------------------------------------------
	result = cf.open(fileName);
	printResult("Call open(\"valid_file\", true) in random access mode", result);	
	//---------------------------------------------------------------------------------------------------	
	{
		bool writeFailed = false;
		for (long long i = cycles - 1; i >= 0; i--) {
			writeFailed = !cf.write(i * messageLength, message, messageLength);
			if (writeFailed) break;
		}
		result = !writeFailed;
		printResult("Multiple reverse writes of short message", result);
	}
	//---------------------------------------------------------------------------------------------------
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
		result = !readFailed;
		printResult("Multiple sequential reads of message and comparing to original message", result);
		delete[] strbuf;
	}
	//---------------------------------------------------------------------------------------------------
	result = cf.close();
	printResult("Call close() file", result);		
	//---------------------------------------------------------------------------------------------------	
	{
		result = !cf.write(cycles * messageLength, message, messageLength);
		printResult("Writes of short message after file closed", result);
	}
	//---------------------------------------------------------------------------------------------------
	{
		char* strbuf = new char[messageLength + 1];
		result = !cf.read(cycles * messageLength, strbuf, messageLength);
		delete[] strbuf;
		printResult("Read of short message after file closed", result);
	}
	//---------------------------------------------------------------------------------------------------
	result = cf.open(fileName);
	printResult("Call open(\"valid_file\", true) in read only mode", result);
	//---------------------------------------------------------------------------------------------------	
	const char* newMessage = "His rot to dust change.";
	const long long newMessageLength = strlen(newMessage);
	{			
		bool writeFailed = false;
		for (long long i = 0; i < cycles; i++) {
			writeFailed = !cf.write(i * newMessageLength, newMessage, newMessageLength);
			if (writeFailed) {
				break;
			}
		}
		result = !writeFailed || (!cf.flush());
		printResult("Multiple sequential overwrites of short message", result);
	}
	//---------------------------------------------------------------------------------------------------
	{		
		char* strbuf = new char[newMessageLength + 1];
		bool readFailed = false;
		for (long long i = 0; i < cycles; i++) {
			readFailed = !cf.read(i * newMessageLength, strbuf, newMessageLength);
			if (readFailed) {
				break;
			}
			strbuf[newMessageLength] = 0; // string null terminator
			if (strcmp(newMessage, strbuf) != 0) {
				readFailed = true;
				break;
			}
		}
		result = !readFailed;
		printResult("Multiple sequential reads of message and comparing to original message", result);
		delete[] strbuf;
	}
	//---------------------------------------------------------------------------------------------------
	{
		size_t fileSize = cf.getFileSize();
		size_t expectedDataSize = (cycles * newMessageLength);
		size_t expectedPages = (expectedDataSize / PAGE_SIZE) + (expectedDataSize % PAGE_SIZE ? 1 : 0);
		size_t expectedFileSize = expectedPages * PAGE_SIZE;
		result = fileSize == expectedFileSize;
		printResult("Checking getFileSize()", result);
	}
	//---------------------------------------------------------------------------------------------------
	{
		size_t filesystemSize = std::filesystem::file_size(fileName);
		result = cf.getFileSize() == filesystemSize;
		printResult("Comparing getFileSize() to std::filesystem", result);
	}
	//std::filesystem::remove(fileName);

	return true;
	/*

	bool open(const char* path, size_t cache = DEFAULT_CACHE, bool readOnly = false);
	bool close();
	bool isOpen();
	bool isReadOnly();

	size_t read(size_t position, void* dataBuffer, size_t length);
	size_t write(size_t position, const void* dataBuffer, size_t length);
	size_t flush();

	void   resetStats();
	double getStats(CachedFileStats type);
	size_t getFileSize();
	size_t getCacheSize();
	size_t setCacheSize(size_t cacheSize);*/
}






/**
*
*  @brief Generates file with data
*  @return sequential write throughput in Mb/s
* 
*/
double CachedFileIOTest::cachedRandomWrites() {

	CachedFileIO cachedFile;
	
	char buf[256] = 
		"\n{\n\t\"name:\": \"unknown\",\n\t\"birthDate\": \"unknown\",\n\t"
		"\"GUID\" : \"6B29FC40-CA47-1067-B31D-00DD010662DA\",\n\t"
		"\"letters\": ['a','b','c','d','e','f','g'],\n\t\"id\": ";

	size_t textLen = strlen(buf);

	size_t length, pos = 0;

	// delete file if exists
	if (std::filesystem::exists(this->fileName)) {
		std::filesystem::remove(this->fileName);
	}
	
	if (!cf.open(this->fileName)) return 0;

	std::cout << "[TEST]  Sequential write " << samplesCount;
	std::cout << " of ~" << textLen + 10 << " byte blocks...\n\t";

	auto startTime = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < samplesCount; i++) {
		_itoa_s(i, &buf[textLen], sizeof(buf)-textLen, 10);
		length = strlen(buf);
		buf[length] = '\n';
		buf[length + 1] = '}';
		buf[length + 2] = '\n';
		length += 3;
		cf.write(pos, buf, length);
		pos += length;
	}

	cf.close();

	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double cachedDuration = duration.count() / 1000000.0;
	double throughput = (pos / (1024.0 * 1024.0)) / (cachedDuration / 1000.0);

	//double cachedDuration = cf.getStats(CachedFileStats::TOTAL_WRITE_TIME_NS) / 1000000.0;
	//double throughput = cf.getStats(CachedFileStats::WRITE_THROUGHPUT);
	std::cout << pos << " bytes (" << cachedDuration << "ms), ";
	std::cout << "Write: " << throughput << " Mb/sec\n\n";

	return throughput;
}


/**
*
*  @brief  Box Muller Method
*  @return normal distributed random number
*
*/
double CachedFileIOTest::randNormal(double mean, double stddev)
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



/**
* 
*  @brief Random reads using cache as 10% size of file
*  @return random read throughput in Mb/s
* 
*/
double CachedFileIOTest::cachedRandomReads() {

	CachedFileIO cachedFile;

	size_t bytesRead = 0;

	if (!cf.open(this->fileName)) return false;
	
	size_t fileSize = cf.getFileSize();
	cf.setCacheSize(size_t(fileSize * cacheRatio));
		
	std::cout << "[TEST]  CACHED & CONCURRENT random read " << samplesCount;
	std::cout << " of " << docSize << " byte blocks...\n\t";
	
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
				this->cachedRandomReadsThread(i, batchSize, length);
			});					
			bytesRead += batchSize * length;
		}
		
	}
	auto endTime = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
	double readTime = duration.count() / 1000000.0;
	double throughput = (bytesRead / (1024.0 * 1024.0)) / (readTime / 1000.0);

	//double readTime = (cf.getStats(CachedFileStats::TOTAL_READ_TIME_NS) / 1000000.0);
	//double throughput = cf.getStats(CachedFileStats::READ_THROUGHPUT);
	std::cout << bytesRead << " bytes (" << readTime << "ms), ";
	std::cout << "Read: " << throughput << " Mb/sec, \n\t";
	std::cout << "Cache Hit: " << cf.getStats(CachedFileStats::CACHE_HITS_RATE) << "%\n\n";

	cf.close();

	return throughput;
}



void CachedFileIOTest::cachedRandomReadsThread(uint64_t batchNo, uint64_t batchSize, uint64_t length) {
	
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
	
	//std::lock_guard iolock(outputLock);
	//std::cout << "Thread batch #" << batchNo << " prcessed " << batchSize << " reads.\n";

}



/**
*
*  @brief Random reads using STDIO
*  @return random read throughput in Mb/s
*/
double CachedFileIOTest::stdioRandomReads() {

	FILE* file = nullptr;

	char* buf = new char[PAGE_SIZE * 4];
	size_t length, pos = 0;
	size_t offset;

	errno_t result = fopen_s(&file, this->fileName, "r+b");
	if (result != 0 || file == nullptr) return -1;

	size_t fileSize = std::filesystem::file_size(this->fileName);

	std::cout << "[TEST]  STDIO random read " << samplesCount << " of " << docSize << " byte blocks...\n\t";
		
	length = docSize;

	std::chrono::steady_clock::time_point startTime, endTime;
	size_t stdioDuration = 0;

	for (size_t i = 0; i < samplesCount; i++) {

		offset = (size_t) (randNormal(0.5, sigma) * double(fileSize - length));

		// offset always positive because its size_t
		if (offset < fileSize) {
			startTime = std::chrono::steady_clock::now();
			_fseeki64(file, offset, SEEK_SET);
			fread(buf, 1, length, file);
			endTime = std::chrono::steady_clock::now();
			stdioDuration += (endTime - startTime).count();
			buf[length + 1] = 0;
			pos += length;
		}

	}

	startTime = std::chrono::steady_clock::now();
	fflush(file);
	endTime = std::chrono::steady_clock::now();
	stdioDuration += (endTime - startTime).count();

	double throughput = (pos / 1024.0 / 1024.0) / (stdioDuration / 1000000000.0);

	std::cout << pos << " bytes (" << stdioDuration / 1000000.0 << "ms), ";
	std::cout << "Read: " << throughput << " Mb/sec\n\n";

	fclose(file);

	delete[] buf;

	return throughput;
}

