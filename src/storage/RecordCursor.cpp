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
*  @brief RecordCursor constructor
*/
RecordCursor::RecordCursor(RecordFileIO& rf, RecordHeader& header, uint64_t position) : recordFile(rf) {
	std::unique_lock lock(cursorMutex);
	memcpy(&recordHeader, &header, RECORD_HEADER_SIZE);
	currentPosition = position;
}


/*
*  @brief Checks if cursor is still valid
*  @returns true if valid, false otherwise
*/
bool RecordCursor::isValid() {
	// Sample record header
	//RecordHeader recordSample;
	uint64_t samplePosition;
	{
		std::unique_lock lockCursor(cursorMutex);
		// Check if cursor invalidated after record deletion
		if (currentPosition == NOT_FOUND) return false;
		// Make sure atomic read		
		recordFile.lockRecord(currentPosition, false);
		samplePosition = recordFile.readRecordHeader(currentPosition, this->recordHeader);
		recordFile.unlockRecord(currentPosition, false);
	}
	// Lock for reading check conditions 
	std::shared_lock lockCursor(cursorMutex);
	bool invalid =
		// Record is corrupt
		(samplePosition == NOT_FOUND) ||
		// Record is deleted
		(recordHeader.bitFlags & RECORD_DELETED_FLAG); 

	if (invalid) {
		std::cout << "HOP!";
	}

	return !invalid;
}



void RecordCursor::invalidate() {
	currentPosition = NOT_FOUND;
	// TODO
}


/*
* @brief Get cursor position
* @return current cursor position in database
*/
uint64_t RecordCursor::getPosition() {	
	std::shared_lock lock(cursorMutex);
	return currentPosition;
}



/*
* @brief Set cursor position
* @param[in] offset - offset from file beginning
* @return true - if offset points to consistent record, false - otherwise
*/
bool RecordCursor::setPosition(uint64_t offset) {
	
	// Try to read record header
	RecordHeader header;
	{
		std::unique_lock lock(cursorMutex);
		recordFile.lockRecord(offset, false);
		uint64_t recPos = recordFile.readRecordHeader(offset, recordHeader);
		recordFile.unlockRecord(offset, false);
		if (recPos == NOT_FOUND || recordHeader.bitFlags & RECORD_DELETED_FLAG) {
			currentPosition = NOT_FOUND;
			return false;
		}
		currentPosition = offset;
	}	
	return true;
}


/*
* @brief Moves cursor to the next record in database
* @return true - if next record exists, false - otherwise
*/
bool RecordCursor::next() {
	uint64_t nextPos;
	{
		std::shared_lock lock(cursorMutex);
		if (currentPosition == NOT_FOUND || recordHeader.next == NOT_FOUND) {
			return false;
		}
		nextPos = recordHeader.next;
		{
			// check if we reached the last record
			std::shared_lock storageHeaderLock(recordFile.headerMutex);
			if (currentPosition == recordFile.storageHeader.lastRecord) {
				return false;
			}
		}
	}
	return setPosition(nextPos);
}



/*
* @brief Moves cursor to the previois record in database
* @return true - if previous record exists, false - otherwise
*/
bool RecordCursor::previous() {
	uint64_t prevPos;
	{
		std::shared_lock lock(cursorMutex);		
		if (currentPosition == NOT_FOUND || recordHeader.previous == NOT_FOUND) return false;
		prevPos = recordHeader.previous;
		{
			// check if we reached the first record
			std::shared_lock storageHeaderLock(recordFile.headerMutex);
			if (currentPosition == recordFile.storageHeader.firstRecord) return false;
		}
	}	
	return setPosition(prevPos);
}




/*
* @brief Get actual data payload length in bytes of current record
* @return returns data payload length in bytes or zero if fails
*/
uint32_t RecordCursor::getDataLength() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition == NOT_FOUND) ? 0 : recordHeader.dataLength;
}



/*
* @brief Get maximum capacity in bytes of current record
* @return returns maximum capacity in bytes or zero if fails
*/
uint32_t RecordCursor::getRecordCapacity() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition == NOT_FOUND) ? 0 : recordHeader.recordCapacity;
}



/*
* @brief Get current record's next neighbour
* @return returns offset of next neighbour or NOT_FOUND if fails
*/
uint64_t RecordCursor::getNextPosition() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition == NOT_FOUND) ? NOT_FOUND : recordHeader.next;
}



/*
* @brief Get current record's previous neighbour
* @return returns offset of previous neighbour or NOT_FOUND if fails
*/
uint64_t RecordCursor::getPrevPosition() {
	std::shared_lock lock(cursorMutex);	
	return (currentPosition == NOT_FOUND) ? NOT_FOUND : recordHeader.previous;
}



/*
* @brief Reads record data in current position and checks consistency
* @param[out] data - pointer to the user buffer
* @param[in]  length - bytes to read to the user buffer
* @return returns true or false if data corrupted
*/
bool RecordCursor::getRecordData(void* data, uint32_t length) {	
	
	// Lock cursor for reading
	std::shared_lock lock(cursorMutex);

	if (currentPosition == NOT_FOUND || length == 0) return false;
	
	uint64_t bytesToRead;
	uint64_t dataOffset;
	{
		//std::shared_lock lock(recordFile.storageMutex);
		recordFile.lockRecord(currentPosition, false);
		recordFile.readRecordHeader(currentPosition, recordHeader);
		bytesToRead = std::min(recordHeader.dataLength, length);
		dataOffset = currentPosition + RECORD_HEADER_SIZE;
		recordFile.cachedFile.read(dataOffset, data, bytesToRead);
		recordFile.unlockRecord(currentPosition, false);
	}
	// check data consistency by checksum
	uint32_t dataCheckSum = recordFile.checksum((uint8_t*)data, bytesToRead);
	if (dataCheckSum != recordHeader.dataChecksum) {
		std::cout << "checksum not equal\n";
		return false;
	}
	
	return true;
}



/*
* @brief Updates record's data in current position.
* if data length exceeds current record capacity,
* then record moves to new place with appropriate capacity.
* @param[in] data - pointer to new data
* @param[in] length - length of data in bytes
* @param[out] result - updated record header information
* @return returns true or false if fails
*/
bool RecordCursor::setRecordData(const void* data, uint32_t length) {
		
	// Lock cursor for changes
	std::unique_lock lockCursor(cursorMutex);	

	if (recordFile.isReadOnly() || currentPosition == NOT_FOUND) return false;

	uint64_t bytesWritten;

	// Lock storage
	//std::unique_lock lockStorage(recordFile.storageMutex);

	// Update header and check is it still valid
	recordFile.lockRecord(currentPosition, false);
	uint64_t pos = recordFile.readRecordHeader(currentPosition, recordHeader);
	recordFile.unlockRecord(currentPosition, false);
	if (pos == NOT_FOUND || recordHeader.bitFlags & RECORD_DELETED_FLAG) return false;
	

	//------------------------------------------------------------------	
	// if there is enough capacity in record
	//------------------------------------------------------------------
	if (length <= recordHeader.recordCapacity) {
		// Update header data length info
		recordHeader.dataLength = length;
		// Update data and header checksum
		recordHeader.dataChecksum = recordFile.checksum((uint8_t*)data, length);		
		recordHeader.headChecksum = recordFile.checksum((uint8_t*)&recordHeader, RECORD_HEADER_PAYLOAD_SIZE);				
		{
			// Lock storage and write record header and data	
			//std::unique_lock lock(recordFile.storageMutex);						
			bytesWritten = recordFile.cachedFile.write(currentPosition, &recordHeader, RECORD_HEADER_SIZE);
			bytesWritten += recordFile.cachedFile.write(currentPosition + RECORD_HEADER_SIZE, data, length);
			return bytesWritten == (RECORD_HEADER_SIZE + length);
		}			
	}

	//------------------------------------------------------------------	
	// if there is not enough record capacity, then move record	
	//------------------------------------------------------------------	
	RecordHeader newRecordHeader;
	uint64_t offset;	
	{
		// lock storage find free record of required length
		offset = recordFile.allocateRecord(length, newRecordHeader);
		if (offset == NOT_FOUND) return false;
	}	

	// Copy record header fields, update data length and checksum
	newRecordHeader.next = recordHeader.next;
	newRecordHeader.previous = recordHeader.previous;
	newRecordHeader.dataLength = length;
	newRecordHeader.dataChecksum = recordFile.checksum((uint8_t*)data, length);	
	newRecordHeader.headChecksum = recordFile.checksum((uint8_t*)&newRecordHeader, RECORD_HEADER_PAYLOAD_SIZE);
		
	{
		// Lock storage 

		// Delete old record and add it to the free records list
		if (!recordFile.addRecordToFreeList(currentPosition)) return NOT_FOUND;
		// if this is first record, then update storage header
		if (recordHeader.previous == NOT_FOUND) {
			{
				std::unique_lock lock(recordFile.headerMutex);
				recordFile.storageHeader.firstRecord = offset;
			}
			recordFile.writeStorageHeader();
		};

		// Write new record header and data to the storage file	
		recordFile.lockRecord(offset, true);	
		bytesWritten = recordFile.cachedFile.write(offset, &newRecordHeader, RECORD_HEADER_SIZE);
		bytesWritten += recordFile.cachedFile.write(offset + RECORD_HEADER_SIZE, data, length);
		recordFile.unlockRecord(offset, true);
	}

	// Update current record and position
	memcpy(&recordHeader, &newRecordHeader, RECORD_HEADER_SIZE);
	currentPosition = offset;

	return bytesWritten == (RECORD_HEADER_SIZE + length);

}

