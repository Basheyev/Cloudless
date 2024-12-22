



#include "RecordFileIO.h"


using namespace Cloudless::Storage;




RecordCursor::RecordCursor(RecordFileIO& rf) : recordFile(rf) {

}



bool RecordCursor::isValid() {
	// Check if cursor invalidated after record deletion
	if (currentPosition == NOT_FOUND) return false;
	// Sample record header
	RecordHeader recordSample;
	uint64_t samplePosition;
	{
		std::shared_lock lock(recordFile.storageMutex);
		samplePosition = recordFile.readRecordHeader(currentPosition, recordSample);
	}
	// Lock for reading check conditions 
	std::shared_lock lock(cursorMutex);
	bool invalid =
		// Record is corrupt
		(samplePosition == NOT_FOUND) ||
		// Record is deleted
		(recordSample.bitFlags & RECORD_DELETED_BIT) ||
		// Record has been changed
		(recordSample.headChecksum != recordHeader.headChecksum);
	
	return !invalid;
}


/*
*
* @brief Get cursor position
* @return current cursor position in database
*
*/
uint64_t RecordCursor::getPosition() {	
	std::shared_lock lock(cursorMutex);
	return currentPosition;
}



/*
*
* @brief Set cursor position
* @param[in] offset - offset from file beginning
* @return true - if offset points to consistent record, false - otherwise
*
*/
bool RecordCursor::setPosition(uint64_t offset) {
	
	// Try to read record header
	RecordHeader header;
	{
		std::shared_lock lock(recordFile.storageMutex);
		if (recordFile.readRecordHeader(offset, header) == NOT_FOUND) return false;
	}
	// If everything is ok - copy to internal buffer
	{
		std::unique_lock lock(cursorMutex);
		memcpy(&recordHeader, &header, sizeof RecordHeader);
		currentPosition = offset;
	}
	return true;
}


/*
*
* @brief Moves cursor to the next record in database
* @return true - if next record exists, false - otherwise
*
*/
bool RecordCursor::next() {
	{
		std::shared_lock lock(cursorMutex);
		if (currentPosition == NOT_FOUND) return false;
		if (recordHeader.next == NOT_FOUND) return false;
	}
	return setPosition(recordHeader.next);
}



/*
*
* @brief Moves cursor to the previois record in database
* @return true - if previous record exists, false - otherwise
*
*/
bool RecordCursor::previous() {
	{
		std::shared_lock lock(cursorMutex);
		if (currentPosition == NOT_FOUND) return false;
		if (recordHeader.previous == NOT_FOUND) return false;
	}
	return setPosition(recordHeader.previous);
}




/*
*
* @brief Get actual data payload length in bytes of current record
* @return returns data payload length in bytes or zero if fails
*
*/
uint32_t RecordCursor::getDataLength() {
	std::shared_lock lock(cursorMutex);
	if (currentPosition == NOT_FOUND) return 0;
	return recordHeader.dataLength;
}



/*
*
* @brief Get maximum capacity in bytes of current record
* @return returns maximum capacity in bytes or zero if fails
*
*/
uint32_t RecordCursor::getRecordCapacity() {
	std::shared_lock lock(cursorMutex);
	if (currentPosition == NOT_FOUND) return 0;
	return recordHeader.recordCapacity;
}



/*
*
* @brief Get current record's next neighbour
* @return returns offset of next neighbour or NOT_FOUND if fails
*
*/
uint64_t RecordCursor::getNextPosition() {
	std::shared_lock lock(cursorMutex);
	if (currentPosition == NOT_FOUND) return NOT_FOUND;
	return recordHeader.next;
}



/*
*
* @brief Get current record's previous neighbour
* @return returns offset of previous neighbour or NOT_FOUND if fails
*
*/
uint64_t RecordCursor::getPrevPosition() {
	std::shared_lock lock(cursorMutex);
	if (currentPosition == NOT_FOUND) return NOT_FOUND;
	return recordHeader.previous;
}



/*
*
* @brief Reads record data in current position and checks consistency
*
* @param[out] data - pointer to the user buffer
* @param[in]  length - bytes to read to the user buffer
*
* @return returns offset of the record or NOT_FOUND if data corrupted
*
*/
bool RecordCursor::getRecordData(void* data, uint32_t length) {
	if (currentPosition == NOT_FOUND || length == 0) return NOT_FOUND;
	{
		std::shared_lock lock(cursorMutex);
		uint64_t bytesToRead = std::min(recordHeader.dataLength, length);
		uint64_t dataOffset = currentPosition + sizeof(RecordHeader);
		{
			std::shared_lock lock(recordFile.storageMutex);
			recordFile.cachedFile.read(dataOffset, data, bytesToRead);
		}
		// check data consistency by checksum
		uint32_t dataCheckSum = recordFile.checksum((uint8_t*)data, bytesToRead);
		if (dataCheckSum != recordHeader.dataChecksum) return NOT_FOUND;
	}
	return currentPosition;
}



/*
*
* @brief Updates record's data in current position.
* if data length exceeds current record capacity,
* then record moves to new place with appropriate capacity.
*
* @param[in] data - pointer to new data
* @param[in] length - length of data in bytes
* @param[out] result - updated record header information
*
* @return returns current offset of record or NOT_FOUND if fails
*
*/
bool RecordCursor::setRecordData(const void* data, uint32_t length) {

	if (recordFile.isReadOnly() || currentPosition == NOT_FOUND) return false;

	// Lock cursor for changes
	std::unique_lock lock(cursorMutex);
	constexpr uint64_t HEADER_SIZE = sizeof(RecordHeader);
	constexpr uint32_t HEADER_PAYLOAD_SIZE = HEADER_SIZE - sizeof(recordHeader.headChecksum);
	uint64_t bytesWritten;

	//------------------------------------------------------------------	
	// if there is enough capacity in record
	//------------------------------------------------------------------
	if (length <= recordHeader.recordCapacity) {
		// Update header data length info
		recordHeader.dataLength = length;
		// Update data and header checksum
		recordHeader.dataChecksum = recordFile.checksum((uint8_t*)data, length);		
		recordHeader.headChecksum = recordFile.checksum((uint8_t*)&recordHeader, HEADER_PAYLOAD_SIZE);				
		{
			// Lock storage and write record header and data	
			std::unique_lock lock(recordFile.storageMutex);						
			bytesWritten = recordFile.cachedFile.write(currentPosition, &recordHeader, HEADER_SIZE);
			bytesWritten += recordFile.cachedFile.write(currentPosition + HEADER_SIZE, data, length);
			return bytesWritten == (HEADER_SIZE + length);
		}			
	}

	//------------------------------------------------------------------	
	// if there is not enough record capacity, then move record	
	//------------------------------------------------------------------	
	RecordHeader newRecordHeader;
	uint64_t offset;	
	{
		// lock storage find free record of required length
		std::unique_lock lock(recordFile.storageMutex);
		offset = recordFile.allocateRecord(length, newRecordHeader);
		if (offset == NOT_FOUND) return false;
	}	

	// Copy record header fields, update data length and checksum
	newRecordHeader.next = recordHeader.next;
	newRecordHeader.previous = recordHeader.previous;
	newRecordHeader.dataLength = length;
	newRecordHeader.dataChecksum = recordFile.checksum((uint8_t*)data, length);	
	newRecordHeader.headChecksum = recordFile.checksum((uint8_t*)&newRecordHeader, HEADER_PAYLOAD_SIZE);
		
	{
		// Lock storage 
		std::unique_lock lock(recordFile.storageMutex);

		// Delete old record and add it to the free records list
		if (!recordFile.addRecordToFreeList(currentPosition)) return NOT_FOUND;
		// if this is first record, then update storage header
		if (recordHeader.previous == NOT_FOUND) {
			recordFile.storageHeader.firstRecord = offset;
			recordFile.writeStorageHeader();
		};

		// Write new record header and data to the storage file		
		bytesWritten = recordFile.cachedFile.write(offset, &newRecordHeader, HEADER_SIZE);
		bytesWritten += recordFile.cachedFile.write(offset + HEADER_SIZE, data, length);
	}

	// Update current record and position
	memcpy(&recordHeader, &newRecordHeader, HEADER_SIZE);
	currentPosition = offset;

	return bytesWritten == (HEADER_SIZE + length);

}

