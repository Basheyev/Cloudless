
#include "RecordFileIO.h"

#include <iostream>

using namespace Cloudless::Storage;

//-----------------------------------------------------------------------------
// Free record list methods
//-----------------------------------------------------------------------------


/*
*  @brief Creates record from the free list (previously deleted records)
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::getFromFreeList(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length, bool createNewRecord) {

	RecordHeader freeRecord{ 0 };
	uint64_t previousRecordPos;
	uint64_t freeRecordOffset;

	// Synchronize all modification in free list
	std::unique_lock freeLock(freeListMutex);

	{
		std::shared_lock lock(headerMutex);
		if (storageHeader.totalFreeRecords == 0) return NOT_FOUND;
		// Read storage last record position before it will be changed by free list
		freeRecord.next = storageHeader.firstFreeRecord;
		freeRecordOffset = freeRecord.next;
	}

	uint64_t maximumIterations = freeLookupDepth.load();
	uint64_t iterationCounter = 0;

	// iterate through free list and check iterations counter
	while (freeRecord.next != NOT_FOUND && iterationCounter < maximumIterations) {

		// Read next free record header
		lockRecord(freeRecordOffset, true);
		readRecordHeader(freeRecordOffset, freeRecord);

		// if record with requested capacity found
		if (freeRecord.recordCapacity >= capacity && (freeRecord.bitFlags & RECORD_DELETED_FLAG)) {

			// Remove free record from the free list
			removeRecordFromFreeList(freeRecord);

			// update last record to point to new record
			RecordHeader previousRecord;

			// connect new record with previous			
			result.recordCapacity = freeRecord.recordCapacity;
			result.dataLength = length;
			result.dataChecksum = checksum((uint8_t*)data, length);
			// turn off "record deleted" bit
			result.bitFlags = freeRecord.bitFlags & (~RECORD_DELETED_FLAG);

			// update storage header last record to new record
			if (createNewRecord) {
				std::shared_lock lock(headerMutex);
				// connect to previous record
				previousRecordPos = storageHeader.lastRecord;

				result.next = NOT_FOUND;
				result.previous = previousRecordPos;					

				lockRecord(previousRecordPos, true);
				readRecordHeader(previousRecordPos, previousRecord);
				previousRecord.next = freeRecordOffset;
				writeRecordHeader(previousRecordPos, previousRecord);
				unlockRecord(previousRecordPos, true);
			}

			// Update record
			result.headChecksum = checksum((uint8_t*)&result, RECORD_HEADER_PAYLOAD_SIZE);
			cachedFile.write(freeRecordOffset, &result, RECORD_HEADER_SIZE);
			cachedFile.write(freeRecordOffset + RECORD_HEADER_SIZE, data, length);
			unlockRecord(freeRecordOffset, true);

			// Update storage header
			if (createNewRecord) {
				std::unique_lock lock(headerMutex);
				storageHeader.lastRecord = freeRecordOffset;
				storageHeader.totalRecords++;
				writeStorageHeader();
			}

			return freeRecordOffset;
		}

		unlockRecord(freeRecordOffset, true);
		freeRecordOffset = freeRecord.next;

		iterationCounter++;
	}
	return NOT_FOUND;
}



/*
*  @brief Put record to the free list
*  @return true - if record added to the free list, false - if not found
*/
bool RecordFileIO::addRecordToFreeList(uint64_t offset) {

	RecordHeader newFreeRecord;
	RecordHeader previousFreeRecord;
	size_t previousFreeRecordOffset;

	lockRecord(offset, false);
	if (readRecordHeader(offset, newFreeRecord) == NOT_FOUND) {
		unlockRecord(offset, false);
		return false;
	}
	unlockRecord(offset, false);

	// Check if already deleted
	if (newFreeRecord.bitFlags & RECORD_DELETED_FLAG) return false;

	{
		std::unique_lock lock(headerMutex);
		// Update previous free record to reference next new free record
		previousFreeRecordOffset = storageHeader.lastFreeRecord;
		// if its first free record, save its offset to the storage header	
		if (storageHeader.firstFreeRecord == NOT_FOUND) {
			storageHeader.firstFreeRecord = offset;
		}
		// save it as last added free record
		storageHeader.lastFreeRecord = offset;
		storageHeader.totalFreeRecords++;
		// save storage header
		writeStorageHeader();
	}
	

	// if free records list is not empty
	if (previousFreeRecordOffset != NOT_FOUND) {
		// Synchronize all modification in free list
		//std::unique_lock freeLock(freeListMutex);

		// load previous last record
		lockRecord(previousFreeRecordOffset, true);
		readRecordHeader(previousFreeRecordOffset, previousFreeRecord);
		// set last recrod next value to recently deleted free record
		previousFreeRecord.next = offset;
		// save previous last record
		writeRecordHeader(previousFreeRecordOffset, previousFreeRecord);
		unlockRecord(previousFreeRecordOffset, true);
	}

	// Update new free record fields	
	newFreeRecord.next = NOT_FOUND;
	// Point to the previous free record position
	newFreeRecord.previous = previousFreeRecordOffset;
	// Set data length and data checksum to zero, then mark as deleted	
	newFreeRecord.dataLength = 0;
	newFreeRecord.dataChecksum = 0;
	// turn on "record deleted" bit
	newFreeRecord.bitFlags |= RECORD_DELETED_FLAG;
	// Save record header
	lockRecord(offset, true);
	writeRecordHeader(offset, newFreeRecord);
	unlockRecord(offset, true);

	return true;
}


/*
*  @brief Remove record from free list and update siblings interlinks
*  @param[in] freeRecord - header of record to remove from free list
*/
void RecordFileIO::removeRecordFromFreeList(RecordHeader& freeRecord) {

	if (!(freeRecord.bitFlags & RECORD_DELETED_FLAG)) {
		std::cerr << "restoring already restored record\n";
		//return;
		throw std::runtime_error("restoring already restored record");
	}

	// Simplify namings and check
	uint64_t leftSiblingOffset = freeRecord.previous;
	uint64_t rightSiblingOffset = freeRecord.next;
	bool leftSiblingExists = (leftSiblingOffset != NOT_FOUND);
	bool rightSiblingExists = (rightSiblingOffset != NOT_FOUND);

	RecordHeader leftSiblingHeader;
	RecordHeader rightSiblingHeader;

	// If both of siblings exists then we removing record in the middle
	if (leftSiblingExists && rightSiblingExists) {
		// If removing in the middle
		lockRecord(leftSiblingOffset, true);
		lockRecord(rightSiblingOffset, true);
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		leftSiblingHeader.next = rightSiblingOffset;
		rightSiblingHeader.previous = leftSiblingOffset;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		unlockRecord(leftSiblingOffset, true);
		unlockRecord(rightSiblingOffset, true);
		{
			std::unique_lock lock(headerMutex);
			storageHeader.totalFreeRecords--;			
			// Persist storage header
			writeStorageHeader();
		}
	}   // if left sibling exists and right is not
	else if (leftSiblingExists) {
		// if removing last free record
		lockRecord(leftSiblingOffset, true);
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		leftSiblingHeader.next = NOT_FOUND;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		unlockRecord(leftSiblingOffset, true);
		{
			std::unique_lock lock(headerMutex);
			storageHeader.lastFreeRecord = leftSiblingOffset;
			storageHeader.totalFreeRecords--;
			// Persist storage header
			writeStorageHeader();			
		}
	}   // if right sibling exists and left is not
	else if (rightSiblingExists) {
		// if removing first free record		
		lockRecord(rightSiblingOffset, true);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		rightSiblingHeader.previous = NOT_FOUND;
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		unlockRecord(rightSiblingOffset, true);
		{
			std::unique_lock lock(headerMutex);
			storageHeader.firstFreeRecord = rightSiblingOffset;
			storageHeader.totalFreeRecords--;
			// Persist storage header
			writeStorageHeader();			
		}
	} else {
		std::unique_lock lock(headerMutex);
		// If removing last free record
		storageHeader.firstFreeRecord = NOT_FOUND;
		storageHeader.lastFreeRecord = NOT_FOUND;
		storageHeader.totalFreeRecords--;
		// Persist storage header
		writeStorageHeader();		
	}

	
}