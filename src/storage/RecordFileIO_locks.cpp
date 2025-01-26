#include "RecordFileIO.h"


using namespace Cloudless::Storage;

/**
*  @brief Locks record by its offset in file
*  @param[in] offset - record position in file
*  @param[in] exclusive - true if unique lock, false if shared lock
*/
void RecordFileIO::lockRecord(uint64_t offset, bool exclusive) {

	std::shared_ptr<RecordLock> recordLock;

	{
		std::shared_lock<std::shared_mutex> mapLock(recordLocksMutex);     // shared lock records map (RAII)
		auto it = recordLocks.find(offset);                        // search record with given offset
		if (it != recordLocks.end()) {                             // if found
			recordLock = it->second;                               // get record lock structure std::shared_ptr
			if (recordLock) recordLock->counter.fetch_add(1);      // if it exists increment pointers counter
		}
	}

	if (!recordLock) {                                             // if record lock is not found
		std::unique_lock<std::shared_mutex> mapLock(recordLocksMutex);     // exclusive lock records map (RAII)
		auto it = recordLocks.find(offset);                        // acknowledge that record lock exists
		if (it == recordLocks.end()) {                             // if record lock not exists
			recordLock = std::make_shared<RecordLock>();           // create new one
			recordLocks[offset] = recordLock;                      // add it to the records mapp
			recordLock->counter.fetch_add(1);                      // increment pointers counter
		}
		else {                                                   // if record lock exists
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
		std::shared_lock<std::shared_mutex> mapLock(recordLocksMutex);    // shared lock records map (RAII)
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
		std::unique_lock<std::shared_mutex> mapLock(recordLocksMutex);	  // exclusive lock records map (RAII)
		if (recordLock->counter.fetch_sub(1) == 1) {              // if no other locks left
			auto it = recordLocks.find(offset);                   // acknowledge that RecordLock exists again
			if (it != recordLocks.end()) {
				recordLocks.erase(offset);                        // erase from records map
			}
		}
	}

}
