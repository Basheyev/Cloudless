/******************************************************************************
*
*  RecordFileIO class header
*
*  RecordFileIO is designed for seamless storage of binary records of
*  arbitary size (max record size limited to 4Gb), accessing records as
*  linked list and reuse space of deleted records. RecordFileIO uses
*  CachedFileIO to cache frequently accessed data.
*
*  Features:
*    - create/read/update/delete records of arbitrary size
*    - navigate records: first, last, next, previous, exact position
*    - reuse space of deleted records
*    - data consistency check (checksum)
*
*  (C) Cloudless, Bolat Basheyev 2022-2024
* 
******************************************************************************/
#pragma once

#include "CachedFileIO.h"

#include <vector>
#include <string>

namespace Cloudless {

	namespace Storage {

		//----------------------------------------------------------------------------
		// Knowledge Storage header signature and version
		//----------------------------------------------------------------------------
		constexpr uint32_t KNOWLEDGE_SIGNATURE = 0x574F4E4B;   // KNOW signature
		constexpr uint32_t KNOWLEDGE_VERSION = 0x00000001;   // Version 1
		constexpr uint64_t RECORD_DELETED_BIT = 1ULL << 63;   // Highest bit

		//----------------------------------------------------------------------------
		// Knowledge Storage header structure (64 bytes)
		//----------------------------------------------------------------------------
		typedef struct {
			uint32_t      signature;           // BSDB signature
			uint32_t      version;             // Format version		
			uint64_t      endOfFile;           // Size of file

			uint64_t      totalRecords;        // Total number of records
			uint64_t      firstRecord;         // First record offset
			uint64_t      lastRecord;          // Last record offset

			uint64_t      totalFreeRecords;    // Total number of free records
			uint64_t      firstFreeRecord;     // First free record offset
			uint64_t      lastFreeRecord;      // Last free record offset
		} StorageHeader;


		//----------------------------------------------------------------------------
		// Record header structure (40 bytes)
		//----------------------------------------------------------------------------	
		typedef struct {
			uint64_t    next;              // Next record position in data file
			uint64_t    previous;          // Previous record position in data file
			uint64_t    bitFlags;          // Record bit flags (31 bit reserved)
			uint32_t    recordCapacity;    // Record capacity in bytes
			uint32_t    dataLength;        // Data length in bytes			
			uint32_t    dataChecksum;      // Checksum for data consistency check		
			uint32_t    headChecksum;      // Checksum for header consistency check
		} RecordHeader;


		//----------------------------------------------------------------------------
		// RecordFileIO
		//----------------------------------------------------------------------------
		class RecordFileIO {
			friend class RecordCursor;
		public:
			RecordFileIO(CachedFileIO& cachedFile, size_t freeDepth = NOT_FOUND);
			~RecordFileIO();

			bool     flush();
			bool     isOpen();
			bool     isReadOnly();

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

			CachedFileIO& cachedFile;
			StorageHeader storageHeader;
			size_t        freeLookupDepth;

			void     createStorageHeader();
			bool     writeStorageHeader();
			bool     loadStorageHeader();

			void     setFreeRecordLookupDepth(uint64_t maxDepth) { freeLookupDepth = maxDepth; }

			uint64_t readRecordHeader(uint64_t offset, RecordHeader& result);
			uint64_t writeRecordHeader(uint64_t offset, RecordHeader& header);

			uint64_t allocateRecord(uint32_t capacity, RecordHeader& result);
			uint64_t createFirstRecord(uint32_t capacity, RecordHeader& result);
			uint64_t appendNewRecord(uint32_t capacity, RecordHeader& result);
			uint64_t getFromFreeList(uint32_t capacity, RecordHeader& result);

			bool     addRecordToFreeList(uint64_t offset);
			void     removeRecordFromFreeList(RecordHeader& freeRecord);

			uint32_t checksum(const uint8_t* data, uint64_t length);
		};

		//----------------------------------------------------------------------------
		// RecordCursor
		//----------------------------------------------------------------------------

		class RecordCursor {
			friend class RecordFileIO;
		public:
			RecordCursor(RecordFileIO& rf);
			bool getRecordData(void* data, uint32_t length);                 // maybe starting position required
			bool setRecordData(const void* data, uint32_t length);           // maybe starting position required
			bool isValid();

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
