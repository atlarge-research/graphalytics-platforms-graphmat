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
    set (GRAPHMAT_DIST_HOME ${GRAPHMAT_HOME}/include/GMDP)
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

find_package(Boost COMPONENTS serialization REQUIRED)
include_directories(${Boost_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIR})
add_definitions (-DHAS_BOOST)

include_directories(SYSTEM "${GRAPHMAT_HOME}/include")
include_directories(SYSTEM "${GRAPHMAT_DIST_HOME}")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -qopenmp -xHost")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -Wall -pedantic -Wextra -Wno-unused-variable -D__TIMING -D__DEBUG")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -ipo")

add_executable (bfs bfs.cpp)
TARGET_LINK_LIBRARIES(bfs ${Boost_LIBRARIES} )

add_executable (pr pr.cpp)
TARGET_LINK_LIBRARIES(pr ${Boost_LIBRARIES} )

add_executable (conn conn.cpp)
TARGET_LINK_LIBRARIES(conn ${Boost_LIBRARIES} )

add_executable (cd cd.cpp)
TARGET_LINK_LIBRARIES(cd ${Boost_LIBRARIES} )

add_executable (lcc lcc.cpp)
TARGET_LINK_LIBRARIES(lcc ${Boost_LIBRARIES} )

add_executable (sssp sssp.cpp)
TARGET_LINK_LIBRARIES(sssp ${Boost_LIBRARIES} )

add_executable (graph_convert ${GRAPHMAT_HOME}/src/graph_converter.cpp)
TARGET_LINK_LIBRARIES(graph_convert ${Boost_LIBRARIES} )
