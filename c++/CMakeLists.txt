cmake_minimum_required(VERSION 3.5)
project(clickhouse-cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER_VERSION 9.4)
add_definitions(-std=c++17)
add_definitions(-Wall)
add_definitions(-Wno-unused)
add_definitions(-Wno-sign-compare)
set(CMAKE_CXX_FLAGS "-O3 -fPIC")

set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--no-as-needed")

include_directories(include)
include_directories(/usr/include/python3.5)
link_directories(lib)

SET(LIBRARY_OUTPUT_PATH ../../)

ADD_LIBRARY(ck SHARED multi_thread_for_test.cpp)
TARGET_LINK_LIBRARIES(ck -lboost_python-py35 -lclickhouse-cpp-lib -pthread)
