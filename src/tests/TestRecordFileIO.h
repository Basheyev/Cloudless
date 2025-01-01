#pragma once

#include <iostream>
#include <sstream>
#include <filesystem>
#include <unordered_map>

#include "CloudlessTests.h"
#include "RecordFileIO.h"

namespace Cloudless {

	namespace Tests {

		class TestRecordFileIO : public ITestCase{
		public:
			std::string getName() const override;
			void init() override;
			void execute() override;
			bool verify() const override;
			void cleanup() override;
		private:
			bool generateData(size_t recordCount);
			bool readAscending(bool verbose);
			bool readDescending(bool verbose);
			bool removeEvenRecords(bool verbose);
			bool insertNewRecords(size_t recordCount);
			bool multithreaded();

			char* fileName;
			size_t samplesCount;
			
			Storage::CachedFileIO cachedFile;
			std::shared_ptr<Storage::RecordFileIO> db;
		};
	}

}