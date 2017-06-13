GRAPHMAT_HOME=/var/scratch/wlngai/graphalytics/runner/app/graphmat/src

module unload openmpi/open64/64/1.10.1 
module unload openmpi/gcc/64/1.10.1 
module load intel/compiler
module load intel-mpi
module load intel/mkl

module list


mkdir -p bin/standard
(cd bin/standard && cmake -DCMAKE_BUILD_TYPE=Release ../../src/main/c -DGRAPHMAT_HOME=$GRAPHMAT_HOME && make all VERBOSE=1)

mkdir -p bin/granula
(cd bin/granula && cmake -DCMAKE_BUILD_TYPE=Release -DGRANULA=1 ../../src/main/c -DGRAPHMAT_HOME=$GRAPHMAT_HOME && make all VERBOSE=1)

rm -f bin/*/CMakeCache.txt
sed -i '40,69d' sh/prepare-benchmark.sh

