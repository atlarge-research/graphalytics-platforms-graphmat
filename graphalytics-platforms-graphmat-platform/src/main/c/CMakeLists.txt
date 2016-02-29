project (PowerGraphApps CXX)
cmake_minimum_required (VERSION 2.6)

if (NOT GRAPHMAT_HOME)
    set (GRAPHMAT_HOME $ENV{GRAPHMAT_HOME})
endif ()

if (NOT GRAPHMAT_DIST_HOME)
    set (GRAPHMAT_DIST_HOME $ENV{GRAPHMAT_DIST_HOME})
endif ()

if (NOT GRAPHMAT_HOME)
    message(FATAL_ERROR "Before running cmake, please specify GRAPHMAT_HOME as environment variable or use -DGRAPHMAT_HOME")
endif ()

if (NOT GRAPHMAT_DIST_HOME)
    set (GRAPHMAT_DIST_HOME ${GRAPHMAT_HOME}/GraphMatDistributedPrimitives)
endif ()

if (GRANULA)
    add_definitions(-DGRANULA=1)
endif ()


set (CMAKE_CXX_COMPILER mpiicpc)


find_package(MPI REQUIRED)
include_directories (${MPI_CXX_INCLUDE_PATH})
set (LIBS ${LIBS} ${MPI_CXX_LIBRARIES})
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MPI_CXX_COMPILE_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MPI_CXX_LINK_FLAGS}")
add_definitions (-DHAS_MPI)


include_directories(SYSTEM "${GRAPHMAT_HOME}/src")
include_directories(SYSTEM "${GRAPHMAT_DIST_HOME}")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -openmp -xHost")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -Wall -pedantic -Wextra -openmp -Wno-unused-variable -D__TIMING")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -ipo")

add_definitions (-DUNSORTED)
add_definitions (-DUNION_PARALLEL_SPA)
add_definitions (-DINTERSECT_PARALLEL_SPA)
add_definitions (-DSPGEMM_PARALLEL_SPA)
add_definitions (-DFSPGEMM_PARALLEL_SPA)

add_executable (bfs bfs.cpp)
add_executable (pr pr.cpp)
add_executable (conn conn.cpp)
add_executable (cd cd.cpp)
add_executable (lcc lcc.cpp)
add_executable (sssp sssp.cpp)
add_executable (graph_convert ${GRAPHMAT_HOME}/graph_utils/graph_convert.cpp)
