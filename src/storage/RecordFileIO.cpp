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
RecordFileIO::RecordFileIO() {
	
}



/*
* @brief RecordFileIO destructor and finalizations
*/
RecordFileIO::~RecordFileIO() {
	{
		std::unique_lock lock(storageMutex);
		if (!cachedFile.isOpen()) return;
		writeStorageHeader();
	}
	flush();
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

	// Set default free record lookup depth (fragmentation/performance)
	freeLookupDepth = DEFAULT_FREE_RECORD_LOOKUP_DEPTH;

	return true;

}


/**
* @brief Open records file
* @return true if records file successfuly opened, false otherwise
*/
bool RecordFileIO::close () {	
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
*
* @brief Get total number of records in storage
* @return total number of records
*
*/
uint64_t RecordFileIO::getTotalRecords() {
	std::shared_lock lock(storageMutex);
	return storageHeader.totalRecords;
}


/*
* 
* @biref Get total number of free (released) records
* @return total number of free records
* 
*/
uint64_t RecordFileIO::getTotalFreeRecords() {
	std::shared_lock lock(storageMutex);
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

	// Lock storage for exclusive writing
	std::unique_lock lock(storageMutex);
		
	// Allocate new record and link to last record
	RecordHeader newRecordHeader;
	uint64_t recordPosition = allocateRecord(length, newRecordHeader);
	if (recordPosition == NOT_FOUND) return nullptr;
		
	// Fill record header fields		
	newRecordHeader.next = NOT_FOUND;                       
	newRecordHeader.dataLength = length;
	newRecordHeader.bitFlags = 0;
	newRecordHeader.dataChecksum = checksum((uint8_t*) data, length);	
	newRecordHeader.headChecksum = checksum((uint8_t*)&newRecordHeader, RECORD_HEADER_PAYLOAD_SIZE);

	// Write record header and data to the storage file		
	cachedFile.write(recordPosition, &newRecordHeader, RECORD_HEADER_SIZE);
	cachedFile.write(recordPosition + RECORD_HEADER_SIZE, data, length);

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
	std::shared_lock lock(storageMutex);

	// Try to read record header
	RecordHeader header;
	uint64_t recPos = readRecordHeader(recordPosition, header);
	if (recPos == NOT_FOUND || header.bitFlags & RECORD_DELETED_FLAG) return nullptr;

	// If everything is ok - create cursor and copy to its internal buffer
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, header, recordPosition);
			
	return recordCursor;

}


/*
*
* @brief Returns cursor to first record in database
* @return Cursor to consistent record, false - otherwise
*
*/
std::shared_ptr<RecordCursor> RecordFileIO::getFirstRecord() {
	uint64_t firstPos;
	
	// Lock storage
	std::shared_lock lock(storageMutex);
	if (storageHeader.firstRecord == NOT_FOUND) return nullptr;
	firstPos = storageHeader.firstRecord;

	// Try to read record header
	RecordHeader header;
	uint64_t recPos = readRecordHeader(firstPos, header);
	if (recPos == NOT_FOUND || header.bitFlags & RECORD_DELETED_FLAG) return nullptr;

	// If everything is ok - create cursor and copy to its internal buffer
	std::shared_ptr<RecordCursor> recordCursor;
	recordCursor = std::make_shared<RecordCursor>(*this, header, firstPos);
	
	return recordCursor;
}


/*
*
* @brief Moves cursor to last record in database
* @return true - if offset points to consistent record, false - otherwise
*
*/
std::shared_ptr<RecordCursor> RecordFileIO::getLastRecord() {
	uint64_t lastPos;
	
	std::shared_lock lock(storageMutex);
	if (storageHeader.lastRecord == NOT_FOUND) return nullptr;
	lastPos = storageHeader.lastRecord;

	// Try to read record header
	RecordHeader header;
	uint64_t recPos = readRecordHeader(lastPos, header);
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

	// Lock storage and cursor for exclusive writing
	std::unique_lock lockStorage(storageMutex);
	std::unique_lock lockCursor(cursor->cursorMutex);

	// Update header and check is it still valid
	uint64_t pos = readRecordHeader(cursor->currentPosition, cursor->recordHeader);
	if (pos == NOT_FOUND || cursor->recordHeader.bitFlags & RECORD_DELETED_FLAG) return false;
				
	// make shortcuts for code readability
	RecordHeader& recordHeader = cursor->recordHeader;
	uint64_t currentPosition = cursor->currentPosition;
	uint64_t leftSiblingOffset = recordHeader.previous;
	uint64_t rightSiblingOffset = recordHeader.next;

	if (storageHeader.lastRecord == storageHeader.firstFreeRecord) {
		throw std::runtime_error("Cyclic reference");
	}

	// check siblings
	bool leftSiblingExists = (leftSiblingOffset != NOT_FOUND);
	bool rightSiblingExists = (rightSiblingOffset != NOT_FOUND);

	RecordHeader leftSiblingHeader;
	RecordHeader rightSiblingHeader;
	RecordHeader* newCursorRecordHeader;
	uint64_t newCursorPosition;

	if (leftSiblingExists && rightSiblingExists) {  
		// removing record in the middle
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		leftSiblingHeader.next = rightSiblingOffset;
		rightSiblingHeader.previous = leftSiblingOffset;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		newCursorPosition = rightSiblingOffset;
		newCursorRecordHeader = &rightSiblingHeader;
	} else if (leftSiblingExists) {		             
		// removing last record
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		leftSiblingHeader.next = NOT_FOUND;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		// update storage header
		storageHeader.lastRecord = leftSiblingOffset;
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		newCursorPosition = leftSiblingOffset;
		newCursorRecordHeader = &leftSiblingHeader;
	} else if (rightSiblingExists) {		         
		// removing first record
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		rightSiblingHeader.previous = NOT_FOUND;
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		// update storage header
		storageHeader.firstRecord = rightSiblingOffset;
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		newCursorPosition = rightSiblingOffset;
		newCursorRecordHeader = &rightSiblingHeader;
	} else {                                         
		// update storage header
		storageHeader.firstRecord = NOT_FOUND;
		storageHeader.lastRecord = NOT_FOUND;
		// add record to free list and mark as deleted
		addRecordToFreeList(currentPosition);
		newCursorPosition = NOT_FOUND;
		newCursorRecordHeader = nullptr;
	}

	// Update storage header information about total records number
	storageHeader.totalRecords--;
	writeStorageHeader();

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
* @brief Initialize in memory storage header for new database
*/
void RecordFileIO::createStorageHeader() {

	storageHeader.signature = KNOWLEDGE_SIGNATURE;
	storageHeader.version = KNOWLEDGE_VERSION;
	storageHeader.endOfFile = STORAGE_HEADER_SIZE;

	storageHeader.totalRecords = 0;
	storageHeader.firstRecord = NOT_FOUND;
	storageHeader.lastRecord = NOT_FOUND;

	storageHeader.totalFreeRecords = 0;
	storageHeader.firstFreeRecord = NOT_FOUND;
	storageHeader.lastFreeRecord = NOT_FOUND;
	
	writeStorageHeader();

}



/*
*  @brief Saves in memory storage header to the file storage
*  @return true - if succeeded, false - if failed
*/
bool RecordFileIO::writeStorageHeader() {

	if (storageHeader.lastRecord == storageHeader.firstFreeRecord) {
		if (storageHeader.lastRecord != NOT_FOUND) {
			throw std::runtime_error("Cyclic reference");
		}
	}

	uint64_t bytesWritten = cachedFile.write(0, &storageHeader, STORAGE_HEADER_SIZE);
	if (bytesWritten != STORAGE_HEADER_SIZE) return false;
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

	memcpy(&storageHeader, &sh, STORAGE_HEADER_SIZE);
	
	return true;
}



/**
*  @brief Read record header at the given file position
*  @param[in] offset - record position in the file
*  @param[out] result - user buffer to load record header
*  @return record offset in file or NOT_FOUND if can't read
*/
uint64_t RecordFileIO::readRecordHeader(uint64_t offset, RecordHeader& header) {
	// Read header
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

	// write header
	uint64_t bytesWritten = cachedFile.write(offset, &header, RECORD_HEADER_SIZE);
	if (bytesWritten != RECORD_HEADER_SIZE) return NOT_FOUND;
	
	// return header offset in file
	return offset;
}


/*
* 
*  @brief Allocates new record from free records list or appends to the ond of file
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::allocateRecord(uint32_t capacity, RecordHeader& result) {

	// if there is no free records yet
	if (storageHeader.firstFreeRecord == NOT_FOUND && storageHeader.lastRecord == NOT_FOUND) {
		// if there is no records at all create first record
		return createFirstRecord(capacity, result);
	} else {
		// look up free list for record of suitable capacity
		uint64_t offset = getFromFreeList(capacity, result);
		// if found, then just return it
		if (offset != NOT_FOUND) return offset;
	}
		
	// if there is no free records, append to the end of file	
	return appendNewRecord(capacity, result);

}


/*
*
*  @brief Creates first record in database
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::createFirstRecord(uint32_t capacity, RecordHeader& result) {
	// clear record header
	memset(&result, 0, RECORD_HEADER_SIZE);
	// set value to capacity
	result.next = NOT_FOUND;
	result.previous = NOT_FOUND;
	result.recordCapacity = capacity;
	result.dataLength = 0;
	// calculate offset right after Storage header
	uint64_t offset = STORAGE_HEADER_SIZE;

	// update storage header
	storageHeader.firstRecord = offset;
    storageHeader.lastRecord = offset;
	storageHeader.endOfFile += RECORD_HEADER_SIZE + capacity;
	storageHeader.totalRecords++;
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
uint64_t RecordFileIO::appendNewRecord(uint32_t capacity, RecordHeader& result) {

	if (capacity == 0) return NOT_FOUND;
		
	// update previous free record
	RecordHeader lastRecord;
	uint64_t freeRecordOffset;

	readRecordHeader(storageHeader.lastRecord, lastRecord);
	lastRecord.next = freeRecordOffset = storageHeader.endOfFile;
	writeRecordHeader(storageHeader.lastRecord, lastRecord);

	result.next = NOT_FOUND;
	result.previous = storageHeader.lastRecord;
	result.recordCapacity = capacity;
	result.bitFlags = 0;
	result.dataLength = 0;

	storageHeader.lastRecord = freeRecordOffset;
	storageHeader.endOfFile += RECORD_HEADER_SIZE + capacity;
	storageHeader.totalRecords++;
	writeStorageHeader();

	return freeRecordOffset;
}


/*
*
*  @brief Creates record from the free list (previously deleted records)
*  @param[in] capacity - requested capacity of record
*  @param[out] result  - record header of created new record
*  @return offset of record in the storage file
*/
uint64_t RecordFileIO::getFromFreeList(uint32_t capacity, RecordHeader& result) {

	if (storageHeader.totalFreeRecords == 0) return NOT_FOUND;

	// If there are free records
	RecordHeader freeRecord;	

	// Read storage last record position before it will be changed by free list
	uint64_t previousRecordPos = storageHeader.lastRecord;

	freeRecord.next = storageHeader.firstFreeRecord;
	uint64_t freeRecordOffset = freeRecord.next;        

	uint64_t maximumIterations = std::min(storageHeader.totalFreeRecords, freeLookupDepth);
	uint64_t iterationCounter = 0;
	// iterate through free list and check iterations counter
	while (freeRecord.next != NOT_FOUND && iterationCounter < maximumIterations) {

		// Read next free record header
		readRecordHeader(freeRecordOffset, freeRecord);
		// if record with requested capacity found
		if (freeRecord.recordCapacity >= capacity && (freeRecord.bitFlags & RECORD_DELETED_FLAG)) {		
			
			// Remove free record from the free list
			removeRecordFromFreeList(freeRecord);			
			// update last record to point to new record
			RecordHeader previousRecord;
			readRecordHeader(previousRecordPos, previousRecord);
			previousRecord.next = freeRecordOffset;			
			writeRecordHeader(previousRecordPos, previousRecord);
			// connect new record with previous
			result.next = NOT_FOUND;
			result.previous = previousRecordPos;
			result.recordCapacity = freeRecord.recordCapacity;
			result.dataLength = 0;

			// turn off "record deleted" bit
			result.bitFlags = freeRecord.bitFlags & (~RECORD_DELETED_FLAG);
						
			// update storage header last record to new record
			storageHeader.lastRecord = freeRecordOffset;
			storageHeader.totalRecords++;
			writeStorageHeader();
			return freeRecordOffset;
		}
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
	
	if (readRecordHeader(offset, newFreeRecord) == NOT_FOUND) return false;

	// Check if already deleted
	if (newFreeRecord.bitFlags & RECORD_DELETED_FLAG) return false;
	
	// Update previous free record to reference next new free record
	size_t previousFreeRecordOffset = storageHeader.lastFreeRecord;
	// if free records list is not empty
	if (previousFreeRecordOffset != NOT_FOUND) {
		// load previous last record
		readRecordHeader(previousFreeRecordOffset, previousFreeRecord);
		// set last recrod next value to recently deleted free record
		previousFreeRecord.next = offset;
		// save previous last record
		writeRecordHeader(previousFreeRecordOffset, previousFreeRecord);
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
	writeRecordHeader(offset, newFreeRecord);

	// if its first free record, save its offset to the storage header	
	if (storageHeader.firstFreeRecord == NOT_FOUND) {
		storageHeader.firstFreeRecord = offset;
	}

	// save it as last added free record
	storageHeader.lastFreeRecord = offset;
	storageHeader.totalFreeRecords++;

	// save storage header
	writeStorageHeader();

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
		//throw std::runtime_error("restoring already restored record");
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
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		leftSiblingHeader.next = rightSiblingOffset;
		rightSiblingHeader.previous = leftSiblingOffset;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
	}   // if left sibling exists and right is not
	else if (leftSiblingExists) {                                    
		// if removing last free record
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		leftSiblingHeader.next = NOT_FOUND;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		storageHeader.lastFreeRecord = leftSiblingOffset;
	}   // if right sibling exists and left is not
	else if (rightSiblingExists) {                                   
		// if removing first free record
		readRecordHeader(rightSiblingOffset, rightSiblingHeader);
		rightSiblingHeader.previous = NOT_FOUND;
		writeRecordHeader(rightSiblingOffset, rightSiblingHeader);
		storageHeader.firstFreeRecord = rightSiblingOffset;
	} else {                                                           
		// If removing last free record
		storageHeader.firstFreeRecord = NOT_FOUND;
		storageHeader.lastFreeRecord = NOT_FOUND;
	}
	// Decrement total free records
	storageHeader.totalFreeRecords--;
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