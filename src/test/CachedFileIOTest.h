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

#include "CachedFileIO.h"

namespace Cloudless {

	class CachedFileIOTest {
	public:
		CachedFileIOTest(const char* path);
		~CachedFileIOTest();
		bool run(size_t samples = 1000000, size_t jsonSize = 479, double cacheRatio = 0.15, double sigma = 0.04);
	private:
		CachedFileIO cf;
		const char* fileName;
		size_t samplesCount;
		size_t docSize;
		double cacheRatio;
		double sigma;

		double cachedRandomPageWrites();
		double cachedRandomWrites();
		double randNormal(double mean, double stddev);
		double cachedRandomReads();
		double stdioRandomReads();
		double cachedRandomPageReads();
		double stdioRandomPageReads();
	};

}
