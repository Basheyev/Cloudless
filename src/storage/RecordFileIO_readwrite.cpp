#include "RecordFileIO.h"

#include <iostream>

using namespace Cloudless::Storage;


/*

BUG:

EDIT ASCENDING CYCLIC REFERENCE!!! counter 15001 is more than Total=5000
Record=5496850 prev=5496309 next=5183472

EDIT ASCENDING CYCLIC REFERENCE!!! counter 15001 is more than Total=5000
Record=9217907 prev=9216796 next=8944398

*/

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
		std::cerr << "Record data read (offset == NOT_FOUND || data == nullptr)\n";
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
		std::cerr << "Record header read failed at pos=" << offset << "\n";
		return NOT_FOUND;
	}

	// check data consistency by checksum
	uint32_t dataCheckSum = checksum((uint8_t*)data, recordHeader.dataLength);
	if (dataCheckSum != recordHeader.dataChecksum) {
		std::cerr << "Record data read failed at pos=" << offset << "\n";
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
		recordHeader.dataChecksum = checksum((uint8_t*)data, length);
		recordHeader.headChecksum = checksum((uint8_t*)&recordHeader, RECORD_HEADER_PAYLOAD_SIZE);

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
	
	// Copy record header
	memcpy(&newRecordHeader, &recordHeader,  RECORD_HEADER_SIZE);	
	// Find free record of required length and write it
	newOffset = allocateRecord(length, newRecordHeader, data, length, false);
	if (newOffset == NOT_FOUND) return NOT_FOUND;		
	// Lock new location
	lockRecord(newOffset, true);

	// Unlock previous location
	unlockRecord(offset, true);
	// Delete old record and add it to the free records list	
	if (!addRecordToFreeList(offset)) return NOT_FOUND;

	// lock and update siblings
	RecordHeader leftSiblingHeader;
	RecordHeader rightSiblingHeader;
	uint64_t leftSiblingOffset = recordHeader.previous;
	uint64_t rightSiblingOffset = recordHeader.next;

	// interconnect with left sibling if exists
	if (leftSiblingOffset != NOT_FOUND) {
		lockRecord(leftSiblingOffset, true);
		readRecordHeader(leftSiblingOffset, leftSiblingHeader);
		leftSiblingHeader.next = newOffset;
		writeRecordHeader(leftSiblingOffset, leftSiblingHeader);
		unlockRecord(leftSiblingOffset, true);
	}

	// interconnect with right sibling if exists
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

	return newOffset;

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