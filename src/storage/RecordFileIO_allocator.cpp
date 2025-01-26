#include "RecordFileIO.h"


using namespace Cloudless::Storage;

//-----------------------------------------------------------------------------
// Record allocations methods
//-----------------------------------------------------------------------------


/*
*
*  @brief Allocates new record from free records list or appends to the ond of file
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @param[in] data     - record data
*  @param[in] length   - record data length
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::allocateRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length, bool updateHeader) {

	bool noFreeRecords;
	{
		std::shared_lock lock(headerMutex);
		noFreeRecords = (storageHeader.firstFreeRecord == NOT_FOUND && storageHeader.lastRecord == NOT_FOUND);
	}

	// if there is no free records yet
	if (noFreeRecords) {
		// if there is no records at all create first record
		return createFirstRecord(capacity, result, data, length);
	}
	else {
		// look up free list for record of suitable capacity
		uint64_t offset = getFromFreeList(capacity, result, data, length, updateHeader);
		// if found, then just return it
		if (offset != NOT_FOUND) return offset;
	}

	// if there is no free records, append to the end of file	
	return appendNewRecord(capacity, result, data, length, updateHeader);

}


/*
*
*  @brief Creates first record in database
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::createFirstRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length) {

	// calculate offset right after Storage header
	uint64_t offset = STORAGE_HEADER_SIZE;

	// Fill record header fields
	result.next = NOT_FOUND;
	result.previous = NOT_FOUND;
	result.dataLength = length;
	result.recordCapacity = capacity;
	result.bitFlags = 0;
	result.dataChecksum = checksum((uint8_t*)data, length);
	result.headChecksum = checksum((uint8_t*)&result, RECORD_HEADER_PAYLOAD_SIZE);

	// update storage header
	{
		std::unique_lock lock(headerMutex);
		storageHeader.firstRecord = offset;
		storageHeader.lastRecord = offset;
		storageHeader.endOfData = offset + RECORD_HEADER_SIZE + capacity;
		storageHeader.totalRecords++;

		lockRecord(offset, true);
		cachedFile.write(offset, &result, RECORD_HEADER_SIZE);
		cachedFile.write(offset + RECORD_HEADER_SIZE, data, length);
		unlockRecord(offset, true);
	}

	// Write record header and data to the storage file		
	writeStorageHeader();

	return offset;
}



/*
*
*  @brief Creates record in the end of storage file
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::appendNewRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length, bool updateHeader) {

	if (capacity == 0) return NOT_FOUND;

	// update previous free record
	RecordHeader lastRecord;
	uint64_t freeRecordOffset;
	uint64_t lastRecordOffset;

	// fill record header
	result.next = NOT_FOUND;
	result.recordCapacity = capacity;
	result.bitFlags = 0;
	result.dataLength = length;
	result.dataChecksum = checksum((uint8_t*)data, length);

	// add record to the end of data and update storage header
	{
		std::unique_lock lock(headerMutex);
		lastRecordOffset = storageHeader.lastRecord;
		freeRecordOffset = storageHeader.endOfData;
		storageHeader.endOfData = freeRecordOffset + RECORD_HEADER_SIZE + capacity;

		if (updateHeader) {
			storageHeader.lastRecord = freeRecordOffset;
			storageHeader.totalRecords++;

			// connect to previous record
			result.previous = lastRecordOffset;
			result.headChecksum = checksum((uint8_t*)&result, RECORD_HEADER_PAYLOAD_SIZE);

			// update last record and connect to new record
			lockRecord(lastRecordOffset, true);
			readRecordHeader(lastRecordOffset, lastRecord);
			lastRecord.next = freeRecordOffset;
			writeRecordHeader(lastRecordOffset, lastRecord);
			unlockRecord(lastRecordOffset, true);
		}

		// write data of new appended record
		lockRecord(freeRecordOffset, true);
		cachedFile.write(freeRecordOffset, &result, RECORD_HEADER_SIZE);
		cachedFile.write(freeRecordOffset + RECORD_HEADER_SIZE, data, length);
		unlockRecord(freeRecordOffset, true);

	}
	writeStorageHeader();

	return freeRecordOffset;
}




