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


hosts=`echo $1 | tr ',' ' '`

bin=`realpath $2`
int_dir=`dirname $bin`/../../intermediate
nhosts=`echo $hosts | wc -w`

for host in $hosts;
do
  # Copy binary
  echo Initialize host $host
  ssh $host mkdir -p `dirname $bin`
  ssh $host mkdir -p $int_dir
  scp $bin $host:$bin
done


for i in `seq 0 1000`;
do
  index=`expr $i % $nhosts + 1`
  host=`echo $hosts | cut -d ' ' -f $index`
  files=$int_dir/*.mtx$i

  if ! ls $files > /dev/null 2>&1;
  then
    break
  fi

  echo Transfering part $i to $host

  # Copy intermediate graph files
  scp $files $host:$int_dir

done


#module load intel/mkl
#module load intel/compiler
#module load intel-mpi


module load intel/mkl
module load intel/compiler
module unload openmpi
module load intel-mpi/64/5.1.2/150 

echo Launching $2 on $1
mpirun -ppn 1 -iface ib0 --host $1 numactl -i all $bin ${@:3} &

echo Running MPIRUN process $!

wait