/******************************************************************************
* 
*  CachedFileIO class test header
* 
*  (C) Bolat Basheyev 2022
* 
******************************************************************************/
#pragma once


#include <cstdio>
#include <iostream>
#include <locale>
#include <chrono>
#include <thread>
#include <filesystem>
#include <mutex>

#include "CloudlessTests.h"
#include "CachedFileIO.h"


namespace Cloudless {

	namespace Tests {

		class TestCachedFileIO : public ITestCase {
		public:
			
			std::string getName() const override;
			void init() override;
			void execute() override;
			bool verify() const override;
			void cleanup() override;

		private:

			Storage::CachedFileIO cf;
			char* fileName;
			size_t samplesCount;
			size_t docSize;
			double cacheRatio;
			double sigma;
			std::mutex outputLock;
			
			void testfileOpen(bool fullCheck);
			void testSequentialWrites(long long cycles, const char* message);
			void testReverseWrites(long long cycles, const char* message);			
			void testSequentialReads(long long cycles, const char* message);
			void testRandomWritesThread(uint64_t batchNo, uint64_t cycles, const char* msg);
			void testIOAfterClose();
			void testFileSize(uint64_t expectedDataSize);

			void printResult(const char* useCase, bool result);
			bool testBasicAPI();

			double testRandomMultithreadWrites();
			double randNormal(double mean, double stddev);
			double cachedRandomReads();
			void cachedRandomReadsThread(uint64_t batchNo, uint64_t batchSize, uint64_t length);
			double stdioRandomReads();
		};
	}
}
