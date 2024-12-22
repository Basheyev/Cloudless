# CMakeList.txt: проект CMake для Cloudless; включите исходный код и определения,
# укажите здесь логику для конкретного проекта.
#
cmake_minimum_required (VERSION 3.8)

# Включение горячей перезагрузки для компиляторов MSVC, если поддерживается.
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

project ("Cloudless")


# Добавьте источник в исполняемый файл этого проекта.
add_executable (
    
    Cloudless     
    
    "src/Cloudless.cpp" 
    "src/Cloudless.h"

    "src/storage/CachedFileIO.cpp"
    "src/storage/CachedFileIO.h"
    "src/storage/RecordFileIO.cpp"
    "src/storage/RecordCursor.cpp"
    "src/storage/RecordFileIO.h"

)


add_executable (
    
    CloudlessTests     
    
    "src/storage/CachedFileIO.cpp"
    "src/storage/CachedFileIO.h"
    "src/storage/RecordFileIO.cpp"
    "src/storage/RecordCursor.cpp"
    "src/storage/RecordFileIO.h"

    "src/tests/CloudlessTests.cpp"
    "src/tests/CloudlessTests.h"
    "src/tests/TestCachedFileIO.cpp"
    "src/tests/TestCachedFileIO.h"
  #  "src/test/RecordFileIOTest.cpp"
  #  "src/test/RecordFileIOTest.h"
    
 )


# Добавим директории
target_include_directories(Cloudless
    PUBLIC ${CMAKE_SOURCE_DIR}/src/explorer
    PUBLIC ${CMAKE_SOURCE_DIR}/src/security
    PUBLIC ${CMAKE_SOURCE_DIR}/src/storage
    PUBLIC ${CMAKE_SOURCE_DIR}/src/sync    
)

# Добавим директории
target_include_directories(CloudlessTests
    PUBLIC ${CMAKE_SOURCE_DIR}/src/explorer
    PUBLIC ${CMAKE_SOURCE_DIR}/src/security
    PUBLIC ${CMAKE_SOURCE_DIR}/src/storage
    PUBLIC ${CMAKE_SOURCE_DIR}/src/sync
    PUBLIC ${CMAKE_SOURCE_DIR}/src/tests
)

# Установка стандарта C++ 20
if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET Cloudless PROPERTY CXX_STANDARD 20)
  set_property(TARGET CloudlessTests PROPERTY CXX_STANDARD 20)
endif()

