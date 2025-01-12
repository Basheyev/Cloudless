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

    "src/libs/civetweb/civetweb.h"
    "src/libs/civetweb/civetweb.c"
    "src/libs/civetweb/CivetServer.h"
    "src/libs/civetweb/CivetServer.cpp"

    "src/storage/CachedFileIO.cpp"
    "src/storage/CachedFileIO.h"
    "src/storage/RecordFileIO.cpp"
    "src/storage/RecordCursor.cpp"
    "src/storage/RecordFileIO.h"

 
 "src/storage/BinaryDirectIO.cpp" )


add_executable (
    
    CloudlessTests     
        
    "src/libs/civetweb/civetweb.h"
    "src/libs/civetweb/civetweb.c"
    "src/libs/civetweb/CivetServer.h"
    "src/libs/civetweb/CivetServer.cpp"
    
    "src/storage/CachedFileIO.cpp"
    "src/storage/CachedFileIO.h"
    "src/storage/RecordFileIO.cpp"
    "src/storage/RecordCursor.cpp"
    "src/storage/RecordFileIO.h"

    "src/tests/CloudlessTests.cpp"
    "src/tests/CloudlessTests.h"
    "src/tests/TestCachedFileIO.cpp"
    "src/tests/TestCachedFileIO.h"
    "src/tests/TestRecordFileIO.cpp"
    "src/tests/TestRecordFileIO.h"
    
   "src/storage/BinaryDirectIO.cpp" )


target_compile_definitions(Cloudless PRIVATE NO_SSL)
target_compile_definitions(CloudlessTests PRIVATE NO_SSL)

# Добавим директории
target_include_directories(Cloudless
    PUBLIC ${CMAKE_SOURCE_DIR}/src/navigator    
    PUBLIC ${CMAKE_SOURCE_DIR}/src/storage    
    PUBLIC ${CMAKE_SOURCE_DIR}/src/libs/civetweb
)

# Добавим директории
target_include_directories(CloudlessTests
    PUBLIC ${CMAKE_SOURCE_DIR}/src/libs/civetweb
    PUBLIC ${CMAKE_SOURCE_DIR}/src/navigator    
    PUBLIC ${CMAKE_SOURCE_DIR}/src/storage    
    PUBLIC ${CMAKE_SOURCE_DIR}/src/tests
)

# Установка стандарта C++ 20
if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET Cloudless PROPERTY CXX_STANDARD 20)
  set_property(TARGET CloudlessTests PROPERTY CXX_STANDARD 20)
endif()


# Define source and destination directories
set(SOURCE_DIR "${CMAKE_SOURCE_DIR}/src/navigator")
set(DEST_DIR "${CMAKE_BINARY_DIR}/navigator")

# Убедимся, что исходная папка существует
if(NOT EXISTS ${SOURCE_DIR})
    message(FATAL_ERROR "Source directory does not exist: ${SOURCE_DIR}")
endif()

# Условие для разных платформ
if(WIN32)
    # Windows: Используем встроенные команды CMake и icacls для управления правами
    add_custom_command(
        OUTPUT ${DEST_DIR}
        COMMAND ${CMAKE_COMMAND} -E remove_directory ${DEST_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${SOURCE_DIR} ${DEST_DIR}
        COMMAND icacls ${DEST_DIR} /grant Everyone:(F) /T /Q
        COMMENT "Copy Navigator folder ${DEST_DIR} for Windows"
    )
else()
    # Linux/Unix: Используем rm и chmod для управления правами
    add_custom_command(
        OUTPUT ${DEST_DIR}
        COMMAND rm -rf ${DEST_DIR}
        COMMAND cp -r ${SOURCE_DIR} ${DEST_DIR}
        COMMAND chmod -R a+rwx ${DEST_DIR}
        COMMENT "Copy Navigator folder ${DEST_DIR} for Linux"
    )
endif()

# Создаем цель, чтобы команда выполнялась при сборке
add_custom_target(copy_navigator ALL DEPENDS ${DEST_DIR})

