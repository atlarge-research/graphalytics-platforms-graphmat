project (PowerGraphApps CXX)
cmake_minimum_required (VERSION 2.6)

include (config.cmake)

if (NOT GRAPHMAT_HOME)
    get_filename_component(CONFIG_PATH config.cmake ABSOLUTE)
    message(FATAL_ERROR "Before running cmake, please specify $GRAPHMAT_HOME in: ${CONFIG_PATH}")
endif ()

set (CMAKE_CXX_COMPILER icpc)

include_directories(SYSTEM "${GRAPHMAT_HOME}/src")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -openmp")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -Wall -pedantic -Wextra -openmp -Wno-unused-variable -D__TIMING")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -ipo")

add_executable (bfs bfs.cpp)
add_executable (pr pr.cpp)
add_executable (conn conn.cpp)
add_executable (cd cd.cpp)
add_executable (lcc lcc.cpp)
add_executable (convert ${GRAPHMAT_HOME}/graph_utils/graph_convert.cpp)
