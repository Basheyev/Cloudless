/******************************************************************************
*
*  CachedFileIO class implementation
*
*  CachedFileIO is designed to improve the performance of file I/O
*  operations by utilizing LRU caching and ensuring thread safety.
*  Almost all real-world applications exhibit some form of locality
*  of reference. Research indicates that a cache size equivalent
*  to 10-15% of the database size can achieve more than 95% cache hits.
*
*  Most JSON documents size are less than 1000 bytes. Most apps database
*  read/write operations ratio is 70% / 30%. Read/write operations are
*  faster when aligned to storage device sector/block size and sequential.
*
*  CachedFileIO LRU/FBW (Linked list + Hashmap) caching strategy gives:
*    - O(1) time complexity of page look up
*    - O(1) time complexity of page insert
*    - O(1) time complexity of page remove
*
*  CachedFileIO vs STDIO performance tests in release mode:
*    - 85%-99% cache read hits leads to 70%-500% performance growth
*    - 75%-84% cache read hits leads to 12%-50% performance growth
*
*  (C) Cloudless, Bolat Basheyev 2022-2024
*
******************************************************************************/

#include "CachedFileIO.h"


using namespace Cloudless::Storage;

/**
* @brief Constructor
*/
CachedFileIO::CachedFileIO() {
	this->readOnly.store(false);
	this->cachePageInfoPool = nullptr;		
	this->cachePageDataPool = nullptr;
	this->maxPagesCount.store(0);
	this->pageCounter.store(0);
	resetStats();
}


/**
* @brief Destructor closes file if it still open
*/
CachedFileIO::~CachedFileIO() {
	this->close();
}


/**
*  @brief Opens file and allocates cache memory
*  @param[in] fileName   - the name of the file to be opened (path)
*  @param[in] cacheSize  - how much memory for cache to allocate (bytes) 
*  @param[in] isReadOnly - if true, write operations are not allowed
*  @return true if file opened, false if can't open file
*/
bool CachedFileIO::open(const char* path, bool isReadOnly, size_t cacheSize) {
	
	// Return if path not specified
	if (path == nullptr) return false;

	// if current file still open, close it
	if (isOpen()) close();	

	{
		// Lock file mutex in synchronized section
		std::lock_guard lock(fileMutex);		
		// Try to open existing file for binary read/update 
		if (!file.open(path, isReadOnly)) return false;
	}

	// Set readOnly flag (atomic)
	this->readOnly.store(isReadOnly);
		
	// Allocated cache
	if (setCacheSize(cacheSize) == NOT_FOUND) {
		close();
		return false;
	}
	
	// Clear statistics
	this->resetStats();

	// file successfuly opened
	return true;
}



/**
*  @brief Closes file, persists changed pages and releases cache memory
*  @return true if file correctly closed, false if file has not been opened
*/
bool CachedFileIO::close() {
	// check if file was opened
	if (!isOpen()) return false;
	// flush buffers if we have write permissions
	if (!readOnly.load()) this->flush();
	// close file
	{
		// Lock file mutex in synchronized section
		std::lock_guard lock(fileMutex);
		file.close();
	}
	// Release memory pool of cached pages
	this->releasePool();		
	return true;
}


/**
*  @brief Checks if file is open
*  @return true - if file open, false - otherwise
*/
bool CachedFileIO::isOpen() const {
	//std::lock_guard lock(fileMutex);
	return file.isOpen();
}



/**
*  @brief Checks if file is read only
*  @return true - if file is read only, false - otherwise
*/
bool CachedFileIO::isReadOnly() const {
	return readOnly.load();
}



/**
*  @brief Read data from cached file
*  @param[in]  position   - offset from beginning of the file
*  @param[out] dataBuffer - data buffer where data copied
*  @param[in]  length     - data amount to read
*  @return total bytes amount actually read to the data buffer
*/
size_t CachedFileIO::read(size_t position, void* dataBuffer, size_t length) {

	// In case we reading one aligned page
	if ((position % PAGE_SIZE == 0) && (length == PAGE_SIZE)) {
		return readPage(position / PAGE_SIZE, dataBuffer);
	}

	// Check if file handler, data buffer and length are not null
	if (!isOpen() || dataBuffer == nullptr || length == 0) return 0;

	// Calculate start and end page number in the file
	size_t firstPageNo = position / PAGE_SIZE;
	size_t lastPageNo = (position + length) / PAGE_SIZE;

	// Initialize local variables
	CachePage* pageInfo = nullptr;
	uint8_t* src = nullptr;
	uint8_t* dst = (uint8_t*)dataBuffer;
	size_t bytesToCopy = 0, bytesRead = 0;
	size_t pageDataLength = 0;

	// Iterate through requested file pages
	for (size_t filePage = firstPageNo; filePage <= lastPageNo; filePage++) {
		
		// Lookup or load file page to cache
		pageInfo = searchPageInCache(filePage);

		// use shared lock for concurrent reads of page
		{
			std::shared_lock readLock(pageInfo->pageMutex);

			// Get cached page description and data
			pageDataLength = pageInfo->availableDataLength;

			// Calculate source pointers and data length to copy
			if (filePage == firstPageNo) {
				// Case 1: if reading first page
				size_t firstPageOffset = position % PAGE_SIZE;
				src = &pageInfo->data[firstPageOffset];
				if (firstPageOffset < pageDataLength)
					if (firstPageOffset + length > pageDataLength)
						bytesToCopy = pageDataLength - firstPageOffset;
					else bytesToCopy = length;
				else bytesToCopy = 0;
			}
			else if (filePage == lastPageNo) {
				// Case 2: if reading last page
				size_t remainingBytes = (position + length) % PAGE_SIZE;
				src = pageInfo->data;
				if (remainingBytes < pageDataLength)
					bytesToCopy = remainingBytes;
				else bytesToCopy = pageDataLength;
			}
			else {
				// Case 3: if reading middle page 
				src = pageInfo->data;
				bytesToCopy = PAGE_SIZE;
			}

			// Copy available data from cache page to user's data buffer 		
			memcpy(dst, src, bytesToCopy);   // copy data to user buffer		
			bytesRead += bytesToCopy;        // increment read bytes counter
			dst += bytesToCopy;              // increment pointer in user buffer
		}
	}

	// Atomic increment bytes read
	this->totalBytesRead.fetch_add(bytesRead);
	// return bytes read
	return bytesRead;
}



/**
*  @brief Writes data to cached file
*  @param[in]  position   - offset from beginning of the file
*  @param[in]  dataBuffer - data buffer with write data
*  @param[in]  length     - data amount to write
*  @return total bytes amount written to the cached file
*/
size_t CachedFileIO::write(size_t position, const void* dataBuffer, size_t length) {

	// Check if file handler, data buffer and length are not null
	if (!isOpen() || this->readOnly.load() || dataBuffer == nullptr || length == 0) return 0;
	
	// Calculate start and end page number in the file
	size_t firstPageNo = position / PAGE_SIZE;
	size_t lastPageNo = (position + length) / PAGE_SIZE;

	// Initialize local variables
	CachePage* pageInfo = nullptr;
	uint8_t* src = (uint8_t*)dataBuffer;
	uint8_t* dst = nullptr;
	size_t bytesToCopy = 0, bytesWritten = 0;
	size_t pageDataLength = 0;
	size_t offset = 0;

	// Iterate through file pages
	for (size_t filePage = firstPageNo; filePage <= lastPageNo; filePage++) {

		// Fetch-before-write (FBW)
		pageInfo = searchPageInCache(filePage);
		
		// Lock for concurrent reading
		{ 
			std::shared_lock pageReadLock(pageInfo->pageMutex);

			// Get cached page description and data
			pageDataLength = pageInfo->availableDataLength;

			// Calculate source pointers and data length to write
			if (filePage == firstPageNo) {
				// Case 1: if writing first page
				offset = position % PAGE_SIZE;
				dst = &pageInfo->data[offset];
				bytesToCopy = std::min(length, PAGE_SIZE - offset);
			}
			else if (filePage == lastPageNo) {
				// Case 2: if writing last page
				offset = 0;
				dst = pageInfo->data;
				bytesToCopy = length - bytesWritten;
			}
			else {
				// Case 3: if reading middle page 
				offset = 0;
				dst = pageInfo->data;
				bytesToCopy = PAGE_SIZE;
			}
		}

		// Copy available data from user's data buffer to cache page 
		{
			// Lock for write
			std::unique_lock pageWriteLock(pageInfo->pageMutex);
			memcpy(dst, src, bytesToCopy);       // copy user buffer data to cache page
			pageInfo->state = PageState::DIRTY;  // mark page as "dirty" (rewritten)
			pageInfo->availableDataLength = std::max(pageDataLength, offset + bytesToCopy);
		}
		bytesWritten += bytesToCopy;         // increment written bytes counter
		src += bytesToCopy;                  // increment pointer in user buffer

	}

	// Atomic increment bytes written
	this->totalBytesWritten.fetch_add(bytesWritten);
	// return bytes written
	return bytesWritten;
}



/**
*  @brief Read page from cached file to user buffer
*  @param[in]  pageNo - file page number
*  @param[out] userPageBuffer - data buffer (Boson::PAGE_SIZE)
*  @return total bytes amount actually read to the data buffer
*/
size_t CachedFileIO::readPage(size_t pageNo, void* userPageBuffer) {

	// Lookup or load file page to cache
	CachePage* pageInfo = searchPageInCache(pageNo);
	size_t availableData;

	// Copy available data from cache page to user's data buffer	
	{
		std::shared_lock pageReadLock(pageInfo->pageMutex);
		uint8_t* src = pageInfo->data;
		uint8_t* dst = (uint8_t*) userPageBuffer;
		availableData = pageInfo->availableDataLength;
		memcpy(dst, src, availableData);
	}

	return availableData;
}



/**
*  @brief Writes page from user buffer to cached file
*  @param[in]  pageNo     - page number to write
*  @param[in]  dataBuffer - data buffer with write data
*  @param[in]  length     - data amount to write
*  @return total bytes amount written to the cached file
*/
size_t CachedFileIO::writePage(size_t pageNo, const void* userPageBuffer) {
	
	// Fetch-before-write (FBW)
	CachePage* pageInfo = searchPageInCache(pageNo);

	// Initialize local variables
	uint8_t* src = (uint8_t*)userPageBuffer;
	uint8_t* dst = pageInfo->data;
	size_t bytesToCopy = PAGE_SIZE;

	// Lock page to write
	{
		std::unique_lock pageWriteLock(pageInfo->pageMutex);
		memcpy(dst, src, bytesToCopy);               // copy user buffer data to cache page
		pageInfo->state = PageState::DIRTY;          // mark page as "dirty" (rewritten)
		pageInfo->availableDataLength = bytesToCopy; // set available data as PAGE_SIZE
	}

	return bytesToCopy;
}



/**
*  @brief Persists all changed cache pages to storage device
*  @return true if all changed cache pages been persisted, false otherwise
*/
bool CachedFileIO::flush() {

	if (!isOpen() || this->readOnly.load()) return 0;

	// Suppose all pages will be persisted
	bool allDirtyPagesPersisted = true;

	// Sort cache list by file page number in ascending order for sequential write
	{
		std::lock_guard cacheLock(cacheMutex);
		cacheList.sort([](const CachePage* cp1, const CachePage* cp2)
			{
				if (cp1->filePageNo == cp2->filePageNo)
					return cp1->filePageNo < cp2->filePageNo;
				return cp1->filePageNo < cp2->filePageNo;
			});

		// Persist pages to storage device
		for (CachePage* node : cacheList) {			
			allDirtyPagesPersisted = allDirtyPagesPersisted && persistCachePage(node);
		}
	}

	// Flush buffers to storage device
	file.flush();

	return allDirtyPagesPersisted;

}



/**
* @brief Reset IO statistics
* @param type - requested stats type
* @return value of stats
*/
void CachedFileIO::resetStats() {
	this->cacheRequests.store(0);
	this->cacheMisses.store(0);
	this->totalBytesRead.store(0);
	this->totalBytesWritten.store(0);
}



/**
* @brief Return IO statistics
* @param type - requested stats type
* @return value of stats
*/
double CachedFileIO::getStats(CachedFileStats type) {

	double totalRequests = (double)cacheRequests.load();
	double totalCacheMisses = (double)cacheMisses.load();
	double seconds = 0;
	double megabytes = 0;

	switch (type) {
	case CachedFileStats::TOTAL_REQUESTS:
		return double(totalRequests);
	case CachedFileStats::TOTAL_CACHE_MISSES:
		return double(cacheMisses.load());
	case CachedFileStats::TOTAL_CACHE_HITS:
		return double(totalRequests - cacheMisses.load());
	case CachedFileStats::TOTAL_BYTES_WRITTEN:
		return double(totalBytesWritten.load());
	case CachedFileStats::TOTAL_BYTES_READ:
		return double(totalBytesRead.load());	
	case CachedFileStats::CACHE_HITS_RATE:
		if (totalRequests == 0) return 0;
		return (totalRequests - totalCacheMisses) / totalRequests * 100.0;
	case CachedFileStats::CACHE_MISSES_RATE:
		if (totalRequests == 0) return 0;
		return totalCacheMisses / totalRequests * 100.0;
	}
	return 0.0;
}



/**
*  @brief Get current file data size
*  @return actual file data size in bytes
*/
size_t CachedFileIO::getFileSize() {
	if (!isOpen()) return 0;
	//std::lock_guard lock(fileMutex);
	return file.size();
}


//=============================================================================
// 
// 
//                       Cached pages control methods
// 
// 
//=============================================================================

/**
*  @brief Get cache size in bytes
*  @return actual cache size in bytes
*/
size_t CachedFileIO::getCacheSize() {
	return this->maxPagesCount.load() * PAGE_SIZE;
}



/**
*  @brief Resize cache at runtime: releases memory and allocate new one
*  @param cacheSize - new cache size
*  @return actual cache size in bytes or NOT_FOUND and closes file if failed to allocate
*/
size_t CachedFileIO::setCacheSize(size_t cacheSize) {

	// Check minimal cache size
	if (cacheSize < MINIMAL_CACHE) cacheSize = MINIMAL_CACHE;

	// check if cache is already allocated
	if (cachePageInfoPool != nullptr) {
		// Persist all changed pages to storage device
		this->flush();
		// Release allocated memory, list and map
		this->releasePool();		
	} 
	
	// Calculate pages count
	this->pageCounter.store(0);
	this->maxPagesCount.store(cacheSize / PAGE_SIZE);

	// Allocate new cache (throws bad_alloc)
	this->allocatePool(this->maxPagesCount.load());
		
	// Reset stats
	this->resetStats();
	// Return cache size in bytes
	return this->maxPagesCount.load() * PAGE_SIZE;
}



/**
* @brief Allocates memory pool for cache pages
*/
void CachedFileIO::allocatePool(size_t pagesToAllocate) {
	std::lock_guard lock(cacheMutex);
	this->cachePageInfoPool = new CachePage[pagesToAllocate];
	this->cachePageDataPool = new CachePageData[pagesToAllocate];
	this->cacheMap.reserve(pagesToAllocate);
}


/**
* @brief Releases memory pool
*/
void CachedFileIO::releasePool() {
	std::lock_guard lock(cacheMutex);
	this->maxPagesCount.store(0);
	this->pageCounter.store(0);
	cacheList.clear();
	cacheMap.clear();
	delete[] cachePageInfoPool;
	delete[] cachePageDataPool;
	cachePageInfoPool = nullptr;
	cachePageDataPool = nullptr;
}


/**
* @brief Allocates cache page from memory pool
*/
CachePage* CachedFileIO::allocatePage() {

	if (pageCounter.load() >= maxPagesCount) return nullptr;
	
	// Increment page counter (atomic)
	uint64_t pageNo = pageCounter.fetch_add(1);
	
	// Allocate memory for cache page from pool
	CachePage* newPage = &cachePageInfoPool[pageNo];

	// Lock page to initialize
	{
		std::unique_lock pageLock(newPage->pageMutex);
		// Clear cache page info fields
		newPage->filePageNo = NOT_FOUND;
		newPage->state = PageState::CLEAN;
		newPage->availableDataLength = 0;
		newPage->data = cachePageDataPool[pageNo].data;
	}

	return newPage;
}


/**
* @brief Returns free page: allocates new or return most aged cache page if page limit reached
* @return new allocated or most aged CachePage pointer
*/
CachePage* CachedFileIO::getFreeCachePage() {
	
	// if not all pages are allocated
	CachePage* freePage = allocatePage();

	// if all pages are allocated then use Least Recent Used
	if (freePage == nullptr) {		

		// lock cache structure
		{
			std::lock_guard cacheLock(cacheMutex);
			// get most aged page - back of the list
			freePage = this->cacheList.back();
			// remove page from list's back				
			this->cacheList.pop_back();
			// remove page from map
			this->cacheMap.erase(freePage->filePageNo);
			// Persist page to storage device			
			if (!persistCachePage(freePage)) {
				throw std::runtime_error("Can't persist cache page to the storage device");
			}			
		}

		// lock page to clear
		{
			std::lock_guard pageLock(freePage->pageMutex);
			// Clear cache page info fields
			freePage->filePageNo = NOT_FOUND;
			freePage->availableDataLength = 0;				
		}				
	}

	// return page reference
	return freePage;
}



/**
* @brief Lookup cache page of requested file page if it exists or loads from storage
* @param requestedFilePageNo - requested file page number
* @return cache page reference of requested file page or returns nullptr
*/
CachePage* CachedFileIO::searchPageInCache(size_t filePageNo) {	
	
	CachePage* cachePage = nullptr;

	// atomic increment total cache lookup requests
	cacheRequests.fetch_add(1);

	// lock critical section to prevent cache structure change while searching and changing
	{		
		std::lock_guard lock(cacheMutex);
		// Search file page in index map	
		auto result = cacheMap.find(filePageNo);
		// if page found in cache, move page to the front of list (LRU):
		if (result != cacheMap.end()) {               
			cachePage = result->second;               
			cacheList.splice(cacheList.begin(), cacheList, cachePage->it); 
			cachePage->it = cacheList.begin();        			
		}
	}

	// return page (hashmap pointer is valid)
	if (cachePage) return cachePage;

	// increment cache misses counter
	cacheMisses.fetch_add(1);
	// try to load page to cache from storage
	return loadPageToCache(filePageNo);
}



/**
*  @brief Loads requested page from storage device to cache and returns cache page
*  @param requestedFilePageNo - file page number to load
*  @return loaded page cache index or nullptr if file is not open.
*/
CachePage* CachedFileIO::loadPageToCache(size_t filePageNo) {

	// get new allocated page or most aged one (remove it from the list)
	CachePage* cachePage = getFreeCachePage();

	// calculate offset and initialize variables
	size_t bytesRead = 0;

	{
		// Lock cache page
		std::unique_lock pageLock(cachePage->pageMutex);					
		// Fetch page from storage device
		bytesRead = file.readPage(filePageNo, (CachePageData*) cachePage->data);
		if (bytesRead < PAGE_SIZE) {
			// Clear remaining part of page
			memset(&cachePage->data[bytesRead], 0, PAGE_SIZE - bytesRead);
		}

		// fill loaded page description info
		cachePage->filePageNo = filePageNo;
		cachePage->state = PageState::CLEAN;
		cachePage->availableDataLength = bytesRead;
			
		// cache list & map lock
		{
			std::lock_guard cacheLock(cacheMutex);
			// Insert cache page into the list and to the hashmap
			cacheList.push_front(cachePage);
			cachePage->it = cacheList.begin();
			cacheMap[filePageNo] = cachePage;
		}
	}

	return cachePage;
}



/**
*  @brief Writes specified cache page to the storage device
*  @param cachePageIndex - page index in the cache
*  @return true - page successfuly persisted, false - write failed or file is not open
*/ 
bool CachedFileIO::persistCachePage(CachePage* cachedPage) {
	
	size_t bytesToWrite = PAGE_SIZE;
	size_t bytesWritten = 0;
		
	{
		// Lock cache page
		std::unique_lock pageLock(cachedPage->pageMutex);
		// if page already been persisted then no need to write page 
		if (cachedPage->state == PageState::CLEAN) return true;
		// Get file page number of cached page and calculate offset in the file	
		size_t offset = cachedPage->filePageNo * PAGE_SIZE;
		// Write page to the file on storage device
		bytesWritten = file.writePage(cachedPage->filePageNo, (CachePageData*)cachedPage->data);
		// Check success
		if (bytesWritten == bytesToWrite) {
			cachedPage->state = PageState::CLEAN;
			return true;
		}
	}

	// if failed to write
	return false;
}


