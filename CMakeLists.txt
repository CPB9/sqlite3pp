project(sqlite3pp CXX)

cmake_minimum_required(VERSION 2.8.7)

add_library(sqlite3pp
    src/sqlite3pp.cpp
    src/sqlite3pp.h
    src/sqlite3ppext.cpp
    src/sqlite3ppext.h
)

target_include_directories(sqlite3pp INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(NOT MSVC)
    set(SQLITE3PP_DEFS -std=c++11)
endif()

target_compile_options(sqlite3pp PUBLIC ${SQLITE3PP_DEFS})

