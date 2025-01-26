/******************************************************************************
*
*  RecordFileIO class implementation
*
*  RecordFileIO is designed for seamless storage of binary records of
*  arbitary size (max record size limited to 4Gb), accessing records as
*  linked list and reuse space of deleted records using Record Cursor. 
*  RecordFileIO uses CachedFileIO to cache frequently accessed data.
*
*  Features:
*    - create/read/delete records of arbitrary size
*    - navigate records: first, last, exact position
*    - reuse space of deleted records
*    - data consistency check (checksum)
*    - thread safety
*
*  (C) Cloudless, Bolat Basheyev 2022-2025
*
******************************************************************************/

#include "RecordFileIO.h"

#include <algorithm>
#include <chrono>
#include <iostream>

using namespace Cloudless::Storage;

/*
* @brief RecordFileIO constructor
*/
RecordFileIO::RecordFileIO() : storageHeader{} {
	// Set default free record lookup depth (fragmentation/performance)
	freeLookupDepth.store(FREE_RECORD_LOOKUP_DEPTH);
}


/*
* @brief RecordFileIO destructor and finalizations
*/
RecordFileIO::~RecordFileIO() {
	if (!cachedFile.isOpen()) return;
	{
		std::unique_lock lock(headerMutex);
		writeStorageHeader();
	}
	flush();
	close();
}



/**
* @brief Open records file
* @return true if records file successfuly opened, false otherwise 
*/
bool RecordFileIO::open(const char* path, bool isReadOnly, size_t cacheSize) {

	std::unique_lock lock(storageMutex);

	// Check if file is open
	if (!cachedFile.open(path, isReadOnly, cacheSize)) {
		const char* msg = "Can't operate on closed cached file.";
		throw std::runtime_error(msg);
	}

	// If file is empty and write is permitted, then write initial storage header
	if (cachedFile.getFileSize() == 0 && !cachedFile.isReadOnly()) {
		std::unique_lock lock(headerMutex);
		createStorageHeader();
	}

	// Try to load storage header
	{
		std::unique_lock lock(headerMutex);
		if (!loadStorageHeader()) {
			const char* msg = "Storage file header is invalid or corrupt.\n";
			throw std::runtime_error(msg);
		}
	}

	return true;

}


/**
* @brief Closes records file
* @return true if records file successfuly closed, false otherwise
*/
bool RecordFileIO::close () {	
	std::unique_lock lock(storageMutex);
	return cachedFile.close();
}


/**
*  @brief Persists all changed cache pages to storage device
*  @return true if all changed cache pages been persisted, false otherwise
*/
bool RecordFileIO::flush() {
	std::unique_lock lock(storageMutex);
	return cachedFile.flush();
}


/**
* @brief Checks if file is open 
* @return true if file is open, or false otherwise
*/
bool RecordFileIO::isOpen() {
	return cachedFile.isOpen();
}


/**
*  @brief Checks if file is read only
*  @return true - if file is read only, false - otherwise
*/
bool RecordFileIO::isReadOnly() {
	return cachedFile.isReadOnly();
}


/**
*  @brief Get current file data size
*  @return actual file data size in bytes
*/
uint64_t RecordFileIO::getFileSize() {
	return cachedFile.getFileSize();
}



/*
*  @brief Resets cache statistics
*/
void RecordFileIO::resetCacheStats() {
	cachedFile.resetStats();
}


/*
*  @brief Returns cache statistics requested value
*  @param[in] type of statistics
*  @return cache statistics requested value
*/
double RecordFileIO::getCacheStats(CachedFileStats type) {
	return cachedFile.getStats(type);
}



/*
* @brief Get total number of records in storage
* @return total number of records
*/
uint64_t RecordFileIO::getTotalRecords() {
	std::shared_lock lock(headerMutex);
	return storageHeader.totalRecords;
}


/*
* @biref Get total number of free (released) records
* @return total number of free records
*/
uint64_t RecordFileIO::getTotalFreeRecords() {
	std::shared_lock lock(headerMutex);
	return storageHeader.totalFreeRecords;
}



/*
* @brief Creates new record in the storage
* @param[in] data - pointer to data
* @param[in] length - length of data in bytes
* @return returns shared pointer to the new record or nullptr if fails
*/
std::shared_ptr<RecordCursor> RecordFileIO::createRecord(const void* data, uint32_t length) {

	// Check if file writes are permitted
	if (cachedFile.isReadOnly()) return nullptr;
	
	// Allocate new record and link to last record
	RecordHeader newRecordHeader;
	uint64_t recordPosition = allocateRecord(length, newRecordHeader, data, length ,true);
	if (recordPosition == NOT_FOUND) return nullptr;
	
	// Create cursor and return it
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, newRecordHeader, recordPosition);

	// Return the cursor of created record
	return recordCursor;
}


/*
* @brief Returns cursor to the specified record in the storage
* @param[in] recordPosition - record position in the storage
* @return returns shared pointer to the consistent record or nullptr if record is not found or corrupt
*/
std::shared_ptr<RecordCursor> RecordFileIO::getRecord(uint64_t recordPosition) {

	// Lock storage for reading (concurrent reads allowed)
	//std::shared_lock lock(storageMutex);

	// Try to read record header
	RecordHeader header;
	lockRecord(recordPosition, false);
	uint64_t recPos = readRecordHeader(recordPosition, header);
	unlockRecord(recordPosition, false);
	if (recPos == NOT_FOUND || header.bitFlags & RECORD_DELETED_FLAG) return nullptr;

	// If everything is ok - create cursor and copy to its internal buffer
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, header, recordPosition);
			
	return recordCursor;

}


/*
* @brief Returns cursor to first record in database
* @return Cursor to consistent record, false - otherwise
*/
std::shared_ptr<RecordCursor> RecordFileIO::getFirstRecord() {
	uint64_t firstPos, recPos;
	RecordHeader header;

	// Lock storage header and first record
	{
		std::shared_lock lock(headerMutex);
		if (storageHeader.firstRecord == NOT_FOUND) return nullptr;
		firstPos = storageHeader.firstRecord;
		lockRecord(firstPos, false);
		recPos = readRecordHeader(firstPos, header);
		unlockRecord(firstPos, false);
	}

	// Try to read record header	
	if (recPos == NOT_FOUND || header.bitFlags & RECORD_DELETED_FLAG) return nullptr;

	// If everything is ok - create cursor and copy to its internal buffer
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, header, firstPos);
	
	return recordCursor;
}


/*
* @brief Moves cursor to last record in database
* @return true - if offset points to consistent record, false - otherwise
*/
std::shared_ptr<RecordCursor> RecordFileIO::getLastRecord() {
	uint64_t lastPos, recPos;
	RecordHeader header;

	// Lock storage header and first record
	{
		std::shared_lock lock(headerMutex);
		if (storageHeader.lastRecord == NOT_FOUND) return nullptr;
		lastPos = storageHeader.lastRecord;
		lockRecord(lastPos, false);
		recPos = readRecordHeader(lastPos, header);
		unlockRecord(lastPos, false);
	}
	
	// Try to read record header	
	if (recPos == NOT_FOUND || header.bitFlags & RECORD_DELETED_FLAG) return nullptr;

	// If everything is ok - create cursor and copy to its internal buffer
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, header, lastPos);

	return recordCursor;
}


/*
* @brief Delete record in cursor position
* @return returns true if record is deleted or false if fails
*/
bool RecordFileIO::removeRecord(std::shared_ptr<RecordCursor> cursor) {

	// FYI: cursor::isValid locks storageMutex so do it before unique lock
	if (cursor == nullptr || cachedFile.isReadOnly()) return false;

	std::unique_lock lockCursor(cursor->cursorMutex);

	// Lock current record
	uint64_t currentPosition = cursor->currentPosition;
	lockRecord(currentPosition, false);
	// Update header and check is it still valid		
	uint64_t pos = readRecordHeader(cursor->currentPosition, cursor->recordHeader);
	// Unlock current record
	unlockRecord(currentPosition, false);
	if (pos == NOT_FOUND || cursor->recordHeader.bitFlags & RECORD_DELETED_FLAG) return false;
				
	// make shortcuts for code readability
	RecordHeader& recordHeader = cursor->recordHeader;	
	uint64_t leftSiblingOffset = recordHeader.previous;
	uint64_t rightSiblingOffset = recordHeader.next;

	// check siblings
	bool leftSiblingExists = (leftSiblingOffset != NOT_FOUND);
	bool rightSiblingExists = (rightSiblingOffset != NOT_FOUND);

	RecordHeader leftSiblingHeader;
	RecordHeader rightSiblingHeader;
	RecordHeader* newCursorRecordHeader;
	uint64_t newCursorPosition;

	if (leftSiblingExists && rightSiblingExists) {  
		// removing record in the middle
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
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		newCursorPosition = rightSiblingOffset;
		newCursorRecordHeader = &rightSiblingHeader;
		{
			// update header
			std::unique_lock lockHeader(headerMutex);
			storageHeader.totalRecords--;
			writeStorageHeader();		}
		
	} else if (leftSiblingExists) {		             
		// removing last record
		lockRecord(leftSiblingOffset, true);		
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		leftSiblingHeader.next = NOT_FOUND;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);		
		unlockRecord(leftSiblingOffset, true);		
		// add record to free list and mark as deleted (contains headerMutex lock & record lock)				
		addRecordToFreeList(currentPosition);		
		{
			// update storage header
			std::unique_lock lockHeader(headerMutex);
			storageHeader.lastRecord = leftSiblingOffset;
			storageHeader.totalRecords--;
			writeStorageHeader();
		}		
		newCursorPosition = leftSiblingOffset;
		newCursorRecordHeader = &leftSiblingHeader;
	} else if (rightSiblingExists) {		         				
		// removing first record		
		lockRecord(rightSiblingOffset, true);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		rightSiblingHeader.previous = NOT_FOUND;
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		unlockRecord(rightSiblingOffset, true);
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		{
			// update storage header			
			std::unique_lock lockHeader(headerMutex);
			storageHeader.firstRecord = rightSiblingOffset;
			storageHeader.totalRecords--;
			writeStorageHeader();
		}		
		newCursorPosition = rightSiblingOffset;
		newCursorRecordHeader = &rightSiblingHeader;
	} else {                         
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);		
		{
			// update storage header
			std::unique_lock lockHeader(headerMutex);
			storageHeader.firstRecord = NOT_FOUND;
			storageHeader.lastRecord = NOT_FOUND;
			storageHeader.totalRecords--;
			writeStorageHeader();
		}		
		newCursorPosition = NOT_FOUND;
		newCursorRecordHeader = nullptr;
	}

	
	// Update cursor position to the neighbour record	
	if (newCursorPosition != NOT_FOUND) {
		memcpy(&cursor->recordHeader, newCursorRecordHeader, RECORD_HEADER_SIZE);
		cursor->currentPosition = newCursorPosition;
	}
	else {
		// invalidate cursor if there is no neighbour records
		cursor->currentPosition = NOT_FOUND;
		cursor->recordHeader.next = NOT_FOUND;
		cursor->recordHeader.previous = NOT_FOUND;
		cursor->recordHeader.dataLength = 0;
		cursor->recordHeader.dataChecksum = 0;
		cursor->recordHeader.headChecksum = 0;		
	}
	
	return true;
}





//=============================================================================
// 
// 
//                       Protected Methods
// 
// 
//=============================================================================


/*
*  @brief Resets error code of RecordFileIO for current thread
*/
void RecordFileIO::resetErrorCode() {
	std::unique_lock lock(errorCodesMutex);
	auto threadID = std::this_thread::get_id();	
	errorCodes[threadID] = RecordErrorCode::SUCCESS;
}


/*
*  @brief Returns error code of RecordFileIO for current thread
*  @return error code
*/
RecordErrorCode RecordFileIO::getErrorCode() {
	std::shared_lock lock(errorCodesMutex);
	auto threadID = std::this_thread::get_id();
	auto it = errorCodes.find(threadID);
	if (it == errorCodes.end()) return RecordErrorCode::SUCCESS;
	return it->second;
}


/*
*  @brief Sets error code of RecordFileIO for current thread
*  @param[in] code - error code
*/
void RecordFileIO::setErrorCode(RecordErrorCode code) {
	std::unique_lock lock(errorCodesMutex);
	auto threadID = std::this_thread::get_id();
	errorCodes[threadID] = code;
}

