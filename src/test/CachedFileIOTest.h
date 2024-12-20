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

#include "CachedFileIO.h"

namespace Cloudless {

	class CachedFileIOTest {
	public:
		CachedFileIOTest(const char* path);
		~CachedFileIOTest();
		bool run(size_t samples = 500000, size_t jsonSize = 479, double cacheRatio = 0.15, double sigma = 0.04);
	private:
		CachedFileIO cf;
		const char* fileName;
		size_t samplesCount;
		size_t docSize;
		double cacheRatio;
		double sigma;
		std::mutex outputLock;

		void printResult(const char* useCase, bool result);
		bool testBasicAPI();

		double cachedRandomWrites();
		double randNormal(double mean, double stddev);
		double cachedRandomReads();
		void cachedRandomReadsThread(uint64_t batchNo, uint64_t batchSize, uint64_t length);
		double stdioRandomReads();
	};

}
