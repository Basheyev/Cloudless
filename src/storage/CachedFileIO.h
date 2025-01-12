/******************************************************************************
*
*  CachedFileIO class header
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
*  (C) Cloudless, Bolat Basheyev 2022-2025
*
******************************************************************************/

#pragma once

#include <cstdint>
#include <cstring>
#include <thread>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <unordered_map>

#ifdef _WIN32
	#define NOMINMAX
	#include <Windows.h>
#else
	#include <fcntl.h>
	#include <unistd.h>
	#include <sys/stat.h>
	#include <cstdlib>
#endif

namespace Cloudless {

	namespace Storage {

		//-------------------------------------------------------------------------
		constexpr uint64_t PAGE_SIZE = 8192;                // 8192 bytes page size
		constexpr uint64_t MINIMAL_CACHE = 256 * 1024;      // 256Kb minimal cache
		constexpr uint64_t DEFAULT_CACHE = 1 * 1024 * 1024; // 1Mb default cache
		constexpr uint64_t NOT_FOUND = 0xFFFFFFFFFFFFFFFF;  // "Not found" signature 
		//-------------------------------------------------------------------------

		enum class PageState : uint32_t {           // Cache Page State
			CLEAN = 0,                              // Page has not been changed
			DIRTY = 1                               // Cache page is rewritten
		};

		using CachePageData = uint8_t[PAGE_SIZE];   // Fixed size cache page 		

		struct CachePage {		
			uint64_t  filePageNo = 0;               // Page number in file
			PageState state = PageState::CLEAN;     // Current page state
			uint64_t  availableDataLength = 0;      // Available amount of data
			uint8_t*  data = nullptr;               // Pointer to data (payload)
			std::list<CachePage*>::iterator it{};   // Cache list node iterator
			std::shared_mutex pageMutex;            // Shared mutex
		};

		//-------------------------------------------------------------------------

		using CacheLinkedList =                     // Double linked list
			std::list<CachePage*>;                  // of cached pages pointers			

		using CachedPagesMap =                      // Hashmap of cached pages
			std::unordered_map<size_t, CachePage*>; // File page No. -> CachePage*           			

		//-------------------------------------------------------------------------

		enum class CachedFileStats : uint32_t {     // CachedFileIO stats types
			TOTAL_REQUESTS,                         // Total requests to cache
			TOTAL_CACHE_MISSES,                     // Total number of cache misses
			TOTAL_CACHE_HITS,                       // Total number of cache hits
			TOTAL_BYTES_WRITTEN,                    // Total bytes written
			TOTAL_BYTES_READ,		                // Total bytes read		
			CACHE_HITS_RATE,                        // Cache hits rate (0-100%)
			CACHE_MISSES_RATE,                      // Cache misses rate (0-100%)		
		};

		//-------------------------------------------------------------------------
		// Binary random access page aligned direct file IO
		//-------------------------------------------------------------------------
		class BinaryDirectIO {
		public:
			BinaryDirectIO() = default;
			~BinaryDirectIO();
			bool open(const char* path, bool isReadOnly = false);
			size_t readPage(size_t pageNo, CachePageData* pageBuffer);
			size_t writePage(size_t pageNo, const CachePageData* pageBuffer);
			size_t size();
			bool flush();
			bool isOpen();
			bool close();			
		private:			
#ifdef _WIN32
			HANDLE fileHandle = INVALID_HANDLE_VALUE;
#else
			int fileDescriptor = -1;
#endif			
			bool writeMode = false;
			std::shared_mutex fileMutex;
		};

		//-------------------------------------------------------------------------
		// Binary random access LRU cached file IO
		//-------------------------------------------------------------------------
		class CachedFileIO {
		public:
			CachedFileIO();
			CachedFileIO(const CachedFileIO&) = delete;
			void operator=(const CachedFileIO&) = delete;
			~CachedFileIO();

			bool open(const char* path, bool readOnly = false, size_t cache = DEFAULT_CACHE);
			bool close();
			bool isOpen();
			bool isReadOnly() const;

			size_t read(size_t position, void* dataBuffer, size_t length);
			size_t write(size_t position, const void* dataBuffer, size_t length);
			bool flush();

			void   resetStats();
			double getStats(CachedFileStats type);
			size_t getFileSize();
			size_t getCacheSize();
			size_t setCacheSize(size_t cacheSize);

		private:

			size_t readPage(size_t pageNo, void* pageBuffer);
			size_t writePage(size_t pageNo, const void* pageBuffer);

			void       allocatePool(size_t pagesCount);
			void       releasePool();
			CachePage* allocatePage();
			CachePage* getFreeCachePage();
			CachePage* searchPageInCache(size_t filePageNo);
			CachePage* readPageToCache(size_t filePageNo);
			bool       writeCachePageToStorage(CachePage* pageInfo);

			std::atomic<uint64_t> maxPagesCount;      // Maximum cache capacity (pages)		
			std::atomic<uint64_t> pageCounter;        // Allocated pages counter

			std::atomic<uint64_t> totalBytesRead;     // Total bytes read
			std::atomic<uint64_t> totalBytesWritten;  // Total bytes written
			std::atomic<uint64_t> cacheRequests;      // Cache requests counter
			std::atomic<uint64_t> cacheMisses;        // Cache misses counter

			BinaryDirectIO    file;			          // Thread safe binary file IO
			std::atomic<bool> readOnly;               // Read only flag

			std::mutex        cacheMutex;             // Cache mutex
			CachedPagesMap    cacheMap;               // Cached pages map 
			CacheLinkedList   cacheList;              // Cached pages double linked list
			CachePage*        cachePageInfoPool;      // Cache pages info memory pool
			CachePageData*    cachePageDataPool;      // Cache pages data memory pool

		};
	}
}