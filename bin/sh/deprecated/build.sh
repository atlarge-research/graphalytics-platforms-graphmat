#!/bin/sh
#
# Copyright 2015 Delft University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
sed -i '40,69d' bin/sh/prepare-benchmark.sh

