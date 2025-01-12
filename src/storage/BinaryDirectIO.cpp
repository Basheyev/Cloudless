/******************************************************************************
*
*  BinaryDirectIO class implementation
*
*  BinaryDirectIO is designed to achieve up to a 30% improvement in the 
*  performance of binary file I/O operations by utilizing OS APIs and 
*  page-aligned I/O to avoid the overhead of stdio. We continue to leverage
*  the OS kernel's write-behind (buffering) and write coalescing (sequential
*  grouping) mechanisms, as they remain more efficient and operate closer
*  to the storage device, ensuring optimal I/O performance and thread safety.
* 
*  (C) Cloudless, Bolat Basheyev 2025
*
******************************************************************************/

#include "CachedFileIO.h"


using namespace Cloudless::Storage;


/**
*  @brief Destructor that closes file if it still open
*/
BinaryDirectIO::~BinaryDirectIO() {
    if (isOpen()) close();
}


/**
*  @brief Opens file in binary random acccess mode
*  @param[in] fileName   - the name of the file to be opened (path)
*  @param[in] isReadOnly - if true, write operations are not allowed
*  @return true if file opened, false if can't open file
*/
bool BinaryDirectIO::open(const char* path, bool isReadOnly) {

    if (isOpen()) close();

    std::unique_lock lock(fileMutex);

    this->writeMode = !isReadOnly;
# ifdef _WIN32
    DWORD accessMode = writeMode ? GENERIC_WRITE | GENERIC_READ : GENERIC_READ;
    DWORD creationDisposition = writeMode ? OPEN_ALWAYS : OPEN_EXISTING;    
    fileHandle = CreateFileA(path, accessMode, 0, nullptr, creationDisposition, 0, nullptr);
    if (fileHandle == INVALID_HANDLE_VALUE) return false;
# else
    int flags = writeMode ? (O_RDWR | O_CREAT) : (O_RDONLY);
    fileDescriptor = ::open(path.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fileDescriptor < 0) return false;
# endif
    return true;
}



/**
*  @brief Read page from file
*  @param[in] pageNo     - sequential number of data page in file
*  @param[in] pageBuffer - cache page data structure pointer
*  @return total bytes read or 0 if read operation failed
*/
size_t BinaryDirectIO::readPage(size_t pageNo, CachePageData* pageBuffer) {
    if (!pageBuffer) return false;

    std::shared_lock lock(fileMutex);

# ifdef _WIN32
    LARGE_INTEGER offset;
    offset.QuadPart = static_cast<LONGLONG>(pageNo * PAGE_SIZE);
    if (!SetFilePointerEx(fileHandle, offset, nullptr, FILE_BEGIN)) return 0;
    DWORD bytesRead = 0;
    if (!ReadFile(fileHandle, pageBuffer, PAGE_SIZE, &bytesRead, nullptr)) return bytesRead;
# else
    off_t offset = static_cast<off_t>(pageNo * PAGE_SIZE);
    if (lseek(fileDescriptor, offset, SEEK_SET) == -1) return 0;    
    ssize_t bytesRead = ::read(fileDescriptor, pageBuffer, PAGE_SIZE);
    if (bytesRead != static_cast<ssize_t>(PAGE_SIZE)) return 0;
# endif
    return bytesRead;
}


/**
*  @brief Write page to file
*  @param[in] pageNo     - sequential number of data page in file
*  @param[in] pageBuffer - cache page data structure pointer
*  @return total bytes written or 0 if read operation failed
*/
size_t BinaryDirectIO::writePage(size_t pageNo, const CachePageData* pageBuffer) {
    if (!writeMode || !pageBuffer) return false;

    std::shared_lock lock(fileMutex);

# ifdef _WIN32
    LARGE_INTEGER offset;
    offset.QuadPart = static_cast<LONGLONG>(pageNo * PAGE_SIZE);
    if (!SetFilePointerEx(fileHandle, offset, nullptr, FILE_BEGIN)) return 0;
    DWORD bytesWritten = 0;
    if (!WriteFile(fileHandle, pageBuffer, PAGE_SIZE, &bytesWritten, nullptr)) return bytesWritten;
# else
    off_t offset = static_cast<off_t>(pageNo * PAGE_SIZE);
    if (lseek(fileDescriptor, offset, SEEK_SET) == -1) return 0;
    ssize_t bytesWritten = ::write(fileDescriptor, pageBuffer, PAGE_SIZE);
    if (bytesWritten != static_cast<ssize_t>(PAGE_SIZE)) return 0;
# endif
    return bytesWritten;
}


/**
*  @brief Get current file data size
*  @return actual file data size in bytes
*/
size_t BinaryDirectIO::size() {

    std::shared_lock lock(fileMutex);

# ifdef _WIN32
    LARGE_INTEGER size;    
    if (!GetFileSizeEx(fileHandle, &size)) return 0;
    return static_cast<size_t>(size.QuadPart);
# else
    struct stat fileStat {};
    if (fstat(fileDescriptor, &fileStat) != 0) return 0;
    return static_cast<size_t>(fileStat.st_size);
# endif
}



/**
*  @brief Flush file buffers to storage device
*  @return true if success, false if fails
*/
bool BinaryDirectIO::flush() {    
    if (isOpen()) {
# ifdef _WIN32
        std::unique_lock lock(fileMutex);
        return FlushFileBuffers(fileHandle) != 0;
# else
        std::unique_lock lock(fileMutex);
        return fsync(fileDescriptor) == 0;
# endif
    }
    return false;
}


/**
*  @brief Checks if file is open
*  @return true - if file open, false - otherwise
*/
bool BinaryDirectIO::isOpen() {

    std::shared_lock lock(fileMutex);

#ifdef _WIN32
    return fileHandle != INVALID_HANDLE_VALUE;
#else
    return fileDescriptor != -1;
#endif
}



/**
*  @brief Closes file, persists changed pages and releases cache memory
*  @return true if file correctly closed, false if file has not been opened
*/
bool BinaryDirectIO::close() {
    
    std::unique_lock lock(fileMutex);

# ifdef _WIN32
    if (fileHandle != INVALID_HANDLE_VALUE) {
        CloseHandle(fileHandle);
        fileHandle = INVALID_HANDLE_VALUE;
    }
# else
    if (fileDescriptor >= 0) {
        ::close(fileDescriptor);
        fileDescriptor = -1;
    }
# endif
    return true;
}


