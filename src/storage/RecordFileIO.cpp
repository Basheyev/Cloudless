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
	writeStorageHeader();	
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
		createStorageHeader();
	}

	// Try to load storage header
	if (!loadStorageHeader()) {
		const char* msg = "Storage file header is invalid or corrupt.\n";
		throw std::runtime_error(msg);
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
	uint64_t recordPosition = allocateRecord(length, newRecordHeader, data, length);
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
		}
		writeStorageHeader();
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
		}
		writeStorageHeader();
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
		}
		writeStorageHeader();
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
		}
		writeStorageHeader();
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

//-----------------------------------------------------------------------------
// Storage header read/write methods
//-----------------------------------------------------------------------------

/*
* @brief Initialize in memory storage header for new database
*/
void RecordFileIO::createStorageHeader() {
	{
		std::unique_lock lock(headerMutex);

		storageHeader.signature = KNOWLEDGE_SIGNATURE;
		storageHeader.version = KNOWLEDGE_VERSION;
		storageHeader.endOfData = STORAGE_HEADER_SIZE;

		storageHeader.totalRecords = 0;
		storageHeader.firstRecord = NOT_FOUND;
		storageHeader.lastRecord = NOT_FOUND;

		storageHeader.totalFreeRecords = 0;
		storageHeader.firstFreeRecord = NOT_FOUND;
		storageHeader.lastFreeRecord = NOT_FOUND;
	}
	writeStorageHeader();
}



/*
*  @brief Saves in memory storage header to the file storage
*  @return true - if succeeded, false - if failed
*/
bool RecordFileIO::writeStorageHeader() {

	uint64_t bytesWritten;
	size_t ratioValue;
	{
		// write header to the storage
		std::shared_lock lock(headerMutex);
		bytesWritten = cachedFile.write(0, &storageHeader, STORAGE_HEADER_SIZE);
		if (bytesWritten != STORAGE_HEADER_SIZE) return false;
		ratioValue = storageHeader.totalFreeRecords / FREE_RECORD_LOOKUP_RATIO;
	}			
	// adjust free page lookup depth	
	size_t value = std::max(FREE_RECORD_LOOKUP_DEPTH, ratioValue);
	freeLookupDepth.store(value);
	return true;
}



/*
*  @brief Loads file storage header to memory storage header
*  @return true - if succeeded, false - if failed
*/
bool RecordFileIO::loadStorageHeader() {		
	
	// read storage header
	StorageHeader sh;
	uint64_t bytesRead = cachedFile.read(0, &sh, STORAGE_HEADER_SIZE);
	if (bytesRead != STORAGE_HEADER_SIZE) return false;
	
	// check signature and version
	if (sh.signature != KNOWLEDGE_SIGNATURE) return false;
	if (sh.version != KNOWLEDGE_VERSION) return false;

	// adjust free page lookup depth	
	size_t ratioValue = sh.totalFreeRecords / FREE_RECORD_LOOKUP_RATIO;
	size_t value = std::max(FREE_RECORD_LOOKUP_DEPTH, ratioValue);
	freeLookupDepth.store(value);

	{
		std::unique_lock lock(headerMutex);
		memcpy(&storageHeader, &sh, STORAGE_HEADER_SIZE);
	}
	
	return true;
}


//-----------------------------------------------------------------------------
// Records read/write methods
//-----------------------------------------------------------------------------

/**
*  @brief Read record header at the given file position
*  @param[in] offset - record position in the file
*  @param[out] result - user buffer to load record header
*  @return record offset in file or NOT_FOUND if can't read
*/
uint64_t RecordFileIO::readRecordHeader(uint64_t offset, RecordHeader& header) {
	// Read header without synchronization - caller responsibility
	uint64_t bytesRead = cachedFile.read(offset, &header, RECORD_HEADER_SIZE);
	if (bytesRead != RECORD_HEADER_SIZE) return NOT_FOUND;
	
	// Check header consistency	
	uint32_t expectedChecksum = checksum((uint8_t*)&header, RECORD_HEADER_PAYLOAD_SIZE);
	if (expectedChecksum != header.headChecksum) return NOT_FOUND;

	return offset;
}



/**
*  @brief Write record header at the given file position
*  @param[in] offset - record position in the file
*  @param[in] result - user buffer to load record header
*  @return record offset in file or NOT_FOUND if can't write
*/
uint64_t RecordFileIO::writeRecordHeader(uint64_t offset, RecordHeader& header) {

	// calculate checksum of header payload	
	header.headChecksum = checksum((uint8_t*)&header, RECORD_HEADER_PAYLOAD_SIZE);

	// write header without synchronization - caller responsibility
	uint64_t bytesWritten = cachedFile.write(offset, &header, RECORD_HEADER_SIZE);
	if (bytesWritten != RECORD_HEADER_SIZE) return NOT_FOUND;
	
	// return header offset in file
	return offset;
}



/*
* @brief Reads record data in current position and checks consistency
* @param[in] offset - record position in the file
* @param[out] data - pointer to the user buffer
* @param[in]  length - bytes to read to the user buffer
* @return returns record position if OK or NOT_FOUND if header or data corrupted
*/
uint64_t RecordFileIO::readRecordData(uint64_t offset, void* data) {
	
	if (offset == NOT_FOUND || data == nullptr) {
		std::cout << "Record data read (offset == NOT_FOUND || data == nullptr)\n";
		return NOT_FOUND;
	}

	RecordHeader recordHeader;
	uint64_t dataOffset;
	bool invalidated = false;
	
	lockRecord(offset, false);
	if (readRecordHeader(offset, recordHeader) != NOT_FOUND) {
		dataOffset = offset + RECORD_HEADER_SIZE;
		cachedFile.read(dataOffset, data, recordHeader.dataLength);
	} else invalidated = true;
	unlockRecord(offset, false);
	
	if (invalidated) {
		std::cout << "Record header read failed \n";
		return NOT_FOUND;
	}

	// check data consistency by checksum
	uint32_t dataCheckSum = checksum((uint8_t*)data, recordHeader.dataLength);
	if (dataCheckSum != recordHeader.dataChecksum) {
		std::cout << "Record data read failed \n";
		return NOT_FOUND;
	}

	return offset;
}



/*
* @brief Updates record's data in current position.
* if data length exceeds current record capacity,
* then record moves to new place with appropriate capacity.
* @param[in] offset - record position in the file
* @param[in] data - pointer to new data
* @param[in] length - length of data in bytes
* @return returns record position if OK or NOT_FOUND if failed to write
*/
uint64_t RecordFileIO::writeRecordData(uint64_t offset, const void* data, uint32_t length) {

	if (isReadOnly() || offset == NOT_FOUND || data == nullptr || length == 0) return NOT_FOUND;

	RecordHeader recordHeader;
	uint64_t bytesWritten;
		
	// exclusive lock record
	lockRecord(offset, true);

	// reads it header
	uint64_t pos = readRecordHeader(offset, recordHeader);
	// if header is corrupt or record deleted - return
	if (pos == NOT_FOUND || recordHeader.bitFlags & RECORD_DELETED_FLAG) {
		unlockRecord(offset, true);
		return NOT_FOUND;
	}


	//------------------------------------------------------------------	
	// if there is enough capacity in record
	//------------------------------------------------------------------
	if (length <= recordHeader.recordCapacity) {
		// Update header data length info
		recordHeader.dataLength = length;
		// Update data and header checksum
		recordHeader.dataChecksum = checksum((uint8_t*) data, length);
		recordHeader.headChecksum = checksum((uint8_t*) &recordHeader, RECORD_HEADER_PAYLOAD_SIZE);
		
		bytesWritten = cachedFile.write(offset, &recordHeader, RECORD_HEADER_SIZE);
		bytesWritten += cachedFile.write(offset + RECORD_HEADER_SIZE, data, length);
		
		unlockRecord(offset, true);
		return offset; 
		
	}

	//------------------------------------------------------------------	
	// if there is not enough record capacity, then move record	
	//------------------------------------------------------------------	
	RecordHeader newRecordHeader;
	uint64_t newOffset;
	{
		// Find free record of required length and write it
		unlockRecord(offset, true);
		newOffset = allocateRecord(length, newRecordHeader, data, length);
		if (newOffset == NOT_FOUND) {
			// unlockRecord(offset, true);
			return NOT_FOUND;
		};		
	}

	// BUG: consistency?
	// TODO: Interconnect siblings! Check algorithm

	// lock and update siblings
	RecordHeader leftSiblingHeader;
	RecordHeader rightSiblingHeader;
	uint64_t leftSiblingOffset = recordHeader.previous;
	uint64_t rightSiblingOffset = recordHeader.next;
	
	lockRecord(newOffset, true);

	if (leftSiblingOffset != NOT_FOUND) {
		lockRecord(leftSiblingOffset, true);
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);		
		leftSiblingHeader.next = newOffset;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		unlockRecord(leftSiblingOffset, true);
	}
	if (rightSiblingOffset != NOT_FOUND) {
		lockRecord(rightSiblingOffset, true);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		rightSiblingHeader.previous = newOffset;
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		unlockRecord(rightSiblingOffset, true);
	}
	
	newRecordHeader.previous = leftSiblingOffset;
	newRecordHeader.next = rightSiblingOffset;
	writeRecordHeader(newOffset, newRecordHeader);	
	unlockRecord(newOffset, true);
	
	
	// Update current record and position
	memcpy(&recordHeader, &newRecordHeader, RECORD_HEADER_SIZE);

	// unlock record
	//unlockRecord(offset, true);

	// Delete old record and add it to the free records list		
	if (!addRecordToFreeList(offset)) return NOT_FOUND;
	
	return newOffset; 
	
}


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
uint64_t RecordFileIO::allocateRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length) {

	bool noFreeRecords;
	{
		std::shared_lock lock(headerMutex);
		noFreeRecords = (storageHeader.firstFreeRecord == NOT_FOUND && storageHeader.lastRecord == NOT_FOUND);
	}

	// if there is no free records yet
	if (noFreeRecords) {
		// if there is no records at all create first record
		return createFirstRecord(capacity, result, data, length);
	} else {
		// look up free list for record of suitable capacity
		uint64_t offset = getFromFreeList(capacity, result, data, length);
		// if found, then just return it
		if (offset != NOT_FOUND) return offset;
	}
		
	// if there is no free records, append to the end of file	
	return appendNewRecord(capacity, result, data, length);

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
uint64_t RecordFileIO::appendNewRecord(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length) {

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
		storageHeader.lastRecord = freeRecordOffset;
		storageHeader.endOfData = freeRecordOffset + RECORD_HEADER_SIZE + capacity;
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

		// write data of new appended record
		lockRecord(freeRecordOffset, true);
		cachedFile.write(freeRecordOffset, &result, RECORD_HEADER_SIZE);
		cachedFile.write(freeRecordOffset + RECORD_HEADER_SIZE, data, length);
		unlockRecord(freeRecordOffset, true);

	}
	writeStorageHeader();

	return freeRecordOffset;
}




//-----------------------------------------------------------------------------
// Free record list methods
//-----------------------------------------------------------------------------


/*
*  @brief Creates record from the free list (previously deleted records)
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::getFromFreeList(uint32_t capacity, RecordHeader& result, const void* data, uint32_t length) {

	RecordHeader freeRecord;
	uint64_t previousRecordPos;
	uint64_t freeRecordOffset;

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
		lockRecord(freeRecordOffset, false);
		readRecordHeader(freeRecordOffset, freeRecord);
		
		// if record with requested capacity found
		if (freeRecord.recordCapacity >= capacity && (freeRecord.bitFlags & RECORD_DELETED_FLAG)) {		
			
			// Remove free record from the free list
			removeRecordFromFreeList(freeRecord);			
			
			// update last record to point to new record
			RecordHeader previousRecord;

			// connect new record with previous
			result.next = NOT_FOUND;			
			result.recordCapacity = freeRecord.recordCapacity;
			result.dataLength = length;
			result.dataChecksum = checksum((uint8_t*)data, length);
			// turn off "record deleted" bit
			result.bitFlags = freeRecord.bitFlags & (~RECORD_DELETED_FLAG);

			// update storage header last record to new record
			{
				std::unique_lock lock(headerMutex);
				previousRecordPos = storageHeader.lastRecord;

				// connect to previous record
				result.previous = previousRecordPos;								
				result.headChecksum = checksum((uint8_t*)&result, RECORD_HEADER_PAYLOAD_SIZE);

				lockRecord(previousRecordPos, true);
				readRecordHeader(previousRecordPos, previousRecord);
				previousRecord.next = freeRecordOffset;
				writeRecordHeader(previousRecordPos, previousRecord);
				unlockRecord(previousRecordPos, true);

				// unlock shared lock
				unlockRecord(freeRecordOffset, false);

				// write data of new appended record				
				lockRecord(freeRecordOffset, true);
				cachedFile.write(freeRecordOffset, &result, RECORD_HEADER_SIZE);
				cachedFile.write(freeRecordOffset + RECORD_HEADER_SIZE, data, length);
				unlockRecord(freeRecordOffset, true);

				storageHeader.lastRecord = freeRecordOffset;
				storageHeader.totalRecords++;
			}
		
			writeStorageHeader();
			
			return freeRecordOffset;
		}
		
		unlockRecord(freeRecordOffset, false);
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
	}
	// save storage header
	writeStorageHeader();

	// if free records list is not empty
	if (previousFreeRecordOffset != NOT_FOUND) {
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
			headerMutex.lock();
			storageHeader.totalFreeRecords--;
			headerMutex.unlock();
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
			headerMutex.lock();
			storageHeader.lastFreeRecord = leftSiblingOffset;
			storageHeader.totalFreeRecords--;
			headerMutex.unlock();
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
			headerMutex.lock();
			storageHeader.firstFreeRecord = rightSiblingOffset;
			storageHeader.totalFreeRecords--;
			headerMutex.unlock();
		}
	} else {                                                           
		headerMutex.lock();
		// If removing last free record
		storageHeader.firstFreeRecord = NOT_FOUND;
		storageHeader.lastFreeRecord = NOT_FOUND;		
		storageHeader.totalFreeRecords--; 
		headerMutex.unlock();
	}
	
	// Persist storage header
	writeStorageHeader();
}



/**
*  @brief Adler-32 checksum algoritm (strightforward and not efficent, but its okay)
*  @param[in] data - byte array of data to be checksummed
*  @param[in] length - length of data in bytes
*  @return 32-bit checksum of given data
*/
uint32_t RecordFileIO::checksum(const uint8_t* data, uint64_t length) {

	const uint32_t MOD_ADLER = 65521;
	uint32_t a = 1, b = 0;
	uint64_t index;
	// Process each byte of the data in order
	for (index = 0; index < length; ++index)
	{
		a = (a + data[index]) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
	}
	return (b << 16) | a;
}




/**
*  @brief Locks record by its offset in file
*  @param[in] offset - record position in file
*  @param[in] exclusive - true if unique lock, false if shared lock
*/
void RecordFileIO::lockRecord(uint64_t offset, bool exclusive) {
	
	std::shared_ptr<RecordLock> recordLock;

	{ 
		std::shared_lock<std::shared_mutex> mapLock(mapMutex);     // shared lock records map (RAII)
		auto it = recordLocks.find(offset);                        // search record with given offset
		if (it != recordLocks.end()) {                             // if found
			recordLock = it->second;                               // get record lock structure std::shared_ptr
			if (recordLock) recordLock->counter.fetch_add(1);      // if it exists increment pointers counter
		}
	} 

	if (!recordLock) {                                             // if record lock is not found
		std::unique_lock<std::shared_mutex> mapLock(mapMutex);     // exclusive lock records map (RAII)
		auto it = recordLocks.find(offset);                        // acknowledge that record lock exists
		if (it == recordLocks.end()) {                             // if record lock not exists
			recordLock = std::make_shared<RecordLock>();           // create new one
			recordLocks[offset] = recordLock;                      // add it to the records mapp
			recordLock->counter.fetch_add(1);                      // increment pointers counter
		} else {                                                   // if record lock exists
			recordLock = it->second;                               // get it
			recordLock->counter.fetch_add(1);                      // increment pointers counter
		}
	}

	if (exclusive) 	                                               // if exclusive lock requested
		recordLock->mutex.lock();		                           // do exclusive lock
	else                                                           // otherwise
		recordLock->mutex.lock_shared();                           // do shared lock
		
}



/**
*  @brief Unlocks record by its offset in file
*  @param[in] offset - record position in file
*  @param[in] exclusive - true if unique lock, false if shared lock
*/
void RecordFileIO::unlockRecord(uint64_t offset, bool exclusive) {
	std::shared_ptr<RecordLock> recordLock;

	{
		std::shared_lock<std::shared_mutex> mapLock(mapMutex);    // shared lock records map (RAII)
		auto it = recordLocks.find(offset);                       // search record with given offset
		if (it == recordLocks.end()) return;                      // if not found - do nothing and return
		recordLock = it->second;                                  // get RecordLock shared_ptr by offset
		if (!recordLock) return;                                  // if not found - do nothing and return
	}

	if (exclusive)                                                // if exclusive lock requested
		recordLock->mutex.unlock();		                          // do exclusive lock
	else                                                          // otherwise
		recordLock->mutex.unlock_shared();                        // do shared lock
			
	{
		std::unique_lock<std::shared_mutex> mapLock(mapMutex);	  // exclusive lock records map (RAII)
		if (recordLock->counter.fetch_sub(1) == 1) {              // if no other locks left
			auto it = recordLocks.find(offset);                   // acknowledge that RecordLock exists again
			if (it != recordLocks.end()) {                        
				recordLocks.erase(offset);                        // erase from records map
			}
		}
	}
	
}


