project(sqlite3pp)

cmake_minimum_required(VERSION 2.8.11)

add_subdirectory(thirdparty/bmcl)

add_library(sqlite3
    src/sqlite3.c
    src/sqlite3.h
)

add_library(sqlite3pp
    src/sqlite3pp.cpp
    src/sqlite3pp.h
    src/sqlite3ppext.cpp
    src/sqlite3ppext.h
    src/sqlite3ext.h
)

target_link_libraries(sqlite3pp
    sqlite3
    bmcl
)

target_include_directories(sqlite3pp INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(NOT MSVC)
    set(SQLITE3PP_DEFS -std=c++11)
endif()

target_compile_options(sqlite3pp PUBLIC ${SQLITE3PP_DEFS})

