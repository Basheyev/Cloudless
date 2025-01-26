#include "RecordFileIO.h"

using namespace Cloudless::Storage;

//-----------------------------------------------------------------------------
// Storage header read/write methods
//-----------------------------------------------------------------------------

/*
* @brief Initialize in memory storage header for new database (not synchronized)
*/
void RecordFileIO::createStorageHeader() {

	storageHeader.signature = KNOWLEDGE_SIGNATURE;
	storageHeader.version = KNOWLEDGE_VERSION;
	storageHeader.endOfData = STORAGE_HEADER_SIZE;

	storageHeader.totalRecords = 0;
	storageHeader.firstRecord = NOT_FOUND;
	storageHeader.lastRecord = NOT_FOUND;

	storageHeader.totalFreeRecords = 0;
	storageHeader.firstFreeRecord = NOT_FOUND;
	storageHeader.lastFreeRecord = NOT_FOUND;
	
	writeStorageHeader();
}



/*
*  @brief Saves in memory storage header to the file storage (not synchronized)
*  @return true - if succeeded, false - if failed
*/
bool RecordFileIO::writeStorageHeader() {

	uint64_t bytesWritten;
	size_t ratioValue;

	bytesWritten = cachedFile.write(0, &storageHeader, STORAGE_HEADER_SIZE);
	if (bytesWritten != STORAGE_HEADER_SIZE) return false;
	ratioValue = storageHeader.totalFreeRecords / FREE_RECORD_LOOKUP_RATIO;

	// adjust free page lookup depth	
	size_t value = std::max(FREE_RECORD_LOOKUP_DEPTH, ratioValue);
	freeLookupDepth.store(value);
	return true;
}



/*
*  @brief Loads file storage header to memory storage header (not synchronized)
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

	memcpy(&storageHeader, &sh, STORAGE_HEADER_SIZE);

	return true;
}


