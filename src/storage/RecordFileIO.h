/******************************************************************************
*
*  RecordFileIO & RecordCursor class header
*
*  RecordFileIO & RecordCursor is designed for seamless storage of binary 
*  records of arbitary size (max record size limited to 4Gb), accessing 
*  records as linked list and reuse space of deleted records. RecordFileIO 
*  uses CachedFileIO to cache frequently accessed data.
*
*  Features:
*    - create/read/update/delete records of arbitrary size
*    - navigate records: first, last, next, previous, exact position
*    - reuse space of deleted records
*    - data consistency check (checksum)
*    - thread safety
*
*  (C) Cloudless, Bolat Basheyev 2022-2024
* 
******************************************************************************/
#pragma once

#include "CachedFileIO.h"

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>



// TODO: StorageHeader - add checksum field
// TODO: RecordHeader - break bitFlags into system/user flags and give access


namespace Cloudless {

	namespace Storage {

		//----------------------------------------------------------------------------
		// Knowledge Storage header signature and version
		//----------------------------------------------------------------------------
		constexpr uint32_t KNOWLEDGE_SIGNATURE = 0x574F4E4B;   // KNOW signature
		constexpr uint32_t KNOWLEDGE_VERSION   = 0x00000001;   // Version 1
		constexpr uint64_t RECORD_DELETED_FLAG = 1ULL << 63;   // Highest bit

		//----------------------------------------------------------------------------
		// Knowledge Storage header structure (64 bytes)
		//----------------------------------------------------------------------------
		struct StorageHeader {
			uint32_t      signature;           // BSDB signature
			uint32_t      version;             // Format version		
			uint64_t      endOfData;           // End of data position

			uint64_t      totalRecords;        // Total number of records
			uint64_t      firstRecord;         // First record offset
			uint64_t      lastRecord;          // Last record offset

			uint64_t      totalFreeRecords;    // Total number of free records
			uint64_t      firstFreeRecord;     // First free record offset
			uint64_t      lastFreeRecord;      // Last free record offset
		};
		
		constexpr uint64_t STORAGE_HEADER_SIZE  = sizeof(StorageHeader);
		constexpr uint64_t FREE_RECORD_LOOKUP_DEPTH = 64; // Minimal search depth is 64
		constexpr uint64_t FREE_RECORD_LOOKUP_RATIO = 10; // Max search depth is 1/10

		//----------------------------------------------------------------------------
		// Record header structure (40 bytes)
		//----------------------------------------------------------------------------	
		struct RecordHeader {
			uint64_t    next;              // Next record position in data file
			uint64_t    previous;          // Previous record position in data file
			uint64_t    bitFlags;          // Record bit flags
			uint32_t    recordCapacity;    // Record capacity in bytes
			uint32_t    dataLength;        // Data length in bytes			
			uint32_t    dataChecksum;      // Checksum for data consistency check		
			uint32_t    headChecksum;      // Checksum for header consistency check
		} ;

		constexpr uint64_t RECORD_HEADER_SIZE = sizeof(RecordHeader);
		constexpr uint32_t RECORD_HEADER_PAYLOAD_SIZE = RECORD_HEADER_SIZE - sizeof(RecordHeader::headChecksum);

		//----------------------------------------------------------------------------
		// Record lock structure
		//----------------------------------------------------------------------------	
		struct RecordLock {			
			std::shared_mutex     mutex;
			std::atomic<int32_t>  counter;						
		};
				
		//----------------------------------------------------------------------------
		// RecordFileIO
		//----------------------------------------------------------------------------
		class RecordFileIO {
			friend class RecordCursor;
		public:
			RecordFileIO();
			RecordFileIO(const RecordFileIO&) = delete;
			void operator=(const RecordFileIO&) = delete;
			~RecordFileIO();

			bool open(const char* path, bool isReadOnly = false, size_t cacheSize = DEFAULT_CACHE);
			bool flush();
			bool isOpen();
			bool isReadOnly();
			bool close();

			uint64_t getFileSize();
			uint64_t getTotalRecords();
			uint64_t getTotalFreeRecords();

			std::shared_ptr<RecordCursor> createRecord(const void* data, uint32_t length);
			std::shared_ptr<RecordCursor> getRecord(uint64_t offset);
			std::shared_ptr<RecordCursor> getFirstRecord();
			std::shared_ptr<RecordCursor> getLastRecord();
			bool removeRecord(std::shared_ptr<RecordCursor> cursor);

		protected:

			std::shared_mutex storageMutex;			
			std::shared_mutex headerMutex;
			std::shared_mutex mapMutex;
			std::unordered_map<uint64_t, std::shared_ptr<RecordLock>> recordLocks;
						
			CachedFileIO  cachedFile;
			StorageHeader storageHeader;
			std::atomic<size_t> freeLookupDepth;

			void     createStorageHeader();
			bool     writeStorageHeader();
			bool     loadStorageHeader();
			
			uint64_t readRecordHeader(uint64_t offset, RecordHeader& result);
			uint64_t writeRecordHeader(uint64_t offset, RecordHeader& header);
			uint64_t readRecordData(uint64_t offset, void* data);
			uint64_t writeRecordData(uint64_t offset, const void* data, uint32_t length);

			uint64_t allocateRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length);
			uint64_t createFirstRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length);
			uint64_t appendNewRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length);
			uint64_t getFromFreeList(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length);

			bool     addRecordToFreeList(uint64_t offset);
			void     removeRecordFromFreeList(RecordHeader& freeRecord);

			uint32_t checksum(const uint8_t* data, uint64_t length);

			void     lockRecord(uint64_t offset, bool exclusive);
			void     unlockRecord(uint64_t offset, bool exclusive);
			
		};


		//----------------------------------------------------------------------------
		// RecordCursor
		//----------------------------------------------------------------------------

		class RecordCursor {
			friend class RecordFileIO;
		public:
			
			RecordCursor(RecordFileIO& rf, RecordHeader& rh, uint64_t position);

			bool getRecordData(void* data);
			bool setRecordData(const void* data, uint32_t length);
			bool isValid();
			void invalidate();

			uint64_t getPosition();
			uint32_t getDataLength();
			uint32_t getRecordCapacity();
			uint64_t getNextPosition();
			uint64_t getPrevPosition();

			bool     next();
			bool     previous();

		protected:

			std::shared_mutex cursorMutex;
			RecordFileIO& recordFile;
			RecordHeader  recordHeader;
			size_t        currentPosition;


			bool setPosition(uint64_t);
		};

	}

}
