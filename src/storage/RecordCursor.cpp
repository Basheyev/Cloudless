/******************************************************************************
*
*  RecordCursor class implementation
*
*  RecordCursor is designed for seamless traversal of records, accessing 
*  records as linked list and reuse space of deleted records. RecordCursor 
*  uses RecordFileIO to access storage.
*
*  Features:
*    - read/update/delete records of arbitrary size
*    - navigate records: next, previous
*    - data consistency check (checksum)
*    - thread safety
*
*  (C) Cloudless, Bolat Basheyev 2022-2025
*
******************************************************************************/



#include "RecordFileIO.h"
#include <iostream>

using namespace Cloudless::Storage;



/*
*  @brief RecordCursor constructor called by RecordFileIO
*/
RecordCursor::RecordCursor(RecordFileIO& rf, RecordHeader& header, uint64_t position) : recordFile(rf) {	
	// No synchronization needed, object is not accessible by other threads
	memcpy(&recordHeader, &header, RECORD_HEADER_SIZE);  
	// Set current position of cursor
	currentPosition.store(position);
}


/*
*  @brief Checks if cursor is still valid by loading its header
*  @returns true if valid, false otherwise
*/
bool RecordCursor::isValid() {

	bool invalid = false;
	uint64_t position = currentPosition.load();

	// Check if cursor invalidated after record deletion
	if (position == NOT_FOUND) return false;
		
	// Make sure synchronized read of header
	{   		 
		std::unique_lock lockCursor(cursorMutex);				
		recordFile.lockRecord(position, false);
		uint64_t actualPosition = recordFile.readRecordHeader(position, this->recordHeader);
		recordFile.unlockRecord(position, false);
		// Check if record invalidated (moved or deleted)
		invalid = (actualPosition == NOT_FOUND) || (recordHeader.bitFlags & RECORD_DELETED_FLAG);
	}

	return !invalid;
}


/*
*  @brief Invalidates cursor called by RecordFileIO
*/
void RecordCursor::invalidate() {	
	currentPosition.store(NOT_FOUND);	
}


/*
* @brief Get cursor position
* @return current cursor position in database
*/
uint64_t RecordCursor::getPosition() {		
	return currentPosition.load();
}


/*
* @brief Set cursor position or invalidates it if fails
* @param[in] offset - offset from file beginning
* @return true - if offset points to consistent record, false - otherwise and invalidates cursor
*/
bool RecordCursor::setPosition(uint64_t offset) {	
	// Store new cursor position
	currentPosition.store(offset);
	// Check if position is valid by loading its header
	if (!isValid()) {
		invalidate();
		return false;
	}
	return true;
}


/*
* @brief Moves cursor to the next record in record storage
* @return true - if next record exists, false - otherwise
*/
bool RecordCursor::next() {

	if (currentPosition.load() == NOT_FOUND) return false;
		
	{
		std::shared_lock lock(cursorMutex);
		recordFile.lockRecord(currentPosition, false);
		uint64_t actualPosition = recordFile.readRecordHeader(currentPosition, recordHeader);
		recordFile.unlockRecord(currentPosition, false);
		if (actualPosition == NOT_FOUND) {
			invalidate();
			return false;
		}
	}
	
	if (recordHeader.next == NOT_FOUND) return false;				
	return setPosition(recordHeader.next);
}



/*
* @brief Moves cursor to the previois record in database
* @return true - if previous record exists, false - otherwise
*/
bool RecordCursor::previous() {

	if (currentPosition.load() == NOT_FOUND) return false;

	{
		std::shared_lock lock(cursorMutex);
		recordFile.lockRecord(currentPosition, false);
		uint64_t actualPosition = recordFile.readRecordHeader(currentPosition, recordHeader);
		recordFile.unlockRecord(currentPosition, false);
		if (actualPosition == NOT_FOUND) {
			invalidate();
			return false;
		}
	}

	if (recordHeader.previous == NOT_FOUND) return false;
	return setPosition(recordHeader.previous);
}




/*
* @brief Get actual data payload length in bytes of current record
* @return returns data payload length in bytes or zero if fails
*/
uint32_t RecordCursor::getDataLength() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition.load() == NOT_FOUND) ? 0 : recordHeader.dataLength;
}



/*
* @brief Get maximum capacity in bytes of current record
* @return returns maximum capacity in bytes or zero if fails
*/
uint32_t RecordCursor::getRecordCapacity() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition.load() == NOT_FOUND) ? 0 : recordHeader.recordCapacity;
}



/*
* @brief Get current record's next neighbour
* @return returns offset of next neighbour or NOT_FOUND if fails
*/
uint64_t RecordCursor::getNextPosition() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition.load() == NOT_FOUND) ? NOT_FOUND : recordHeader.next;
}



/*
* @brief Get current record's previous neighbour
* @return returns offset of previous neighbour or NOT_FOUND if fails
*/
uint64_t RecordCursor::getPrevPosition() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition.load() == NOT_FOUND) ? NOT_FOUND : recordHeader.previous;
}



/*
* @brief Reads record data in current position and checks consistency
* @param[out] data - pointer to the user buffer
* @return returns true or false if data corrupted
*/
bool RecordCursor::getRecordData(void* data) {		
	return recordFile.readRecordData(currentPosition.load(), data) != NOT_FOUND;
}



/*
* @brief Updates record's data in current position.
* if data length exceeds current record capacity,
* then record moves to new place with appropriate capacity.
* @param[in] data - pointer to new data
* @param[in] length - length of data in bytes
* @return returns true or false if fails
*/
bool RecordCursor::setRecordData(const void* data, uint32_t length) {
	// Write data to the record
	uint64_t actualPosition = recordFile.writeRecordData(currentPosition.load(), data, length);
	if (actualPosition == NOT_FOUND) return false;
	// if record position changed after update
	if (currentPosition.load() != actualPosition) {
		return setPosition(actualPosition);
	}
	return true;
}

