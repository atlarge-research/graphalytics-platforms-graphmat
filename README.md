# Graphalytics Graphmat platform driver

[![Build Status](https://jenkins.tribler.org/buildStatus/icon?job=Graphalytics/Platforms/GraphMat_master)](https://jenkins.tribler.org/job/Graphalytics/job/Platforms/job/GraphMat_master/)



### Getting started
This is a [Graphalytics](https://github.com/ldbc/ldbc_graphalytics/) benchmark driver for the GraphMat graph-processing platform. Visit the [GraphMat repository](https://github.com/narayanan2004/GraphMat) for more information on this platform.

  - Make sure that you have [installed Graphalytics](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation%3A-Software-Build#the-core-repository). 
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from  `graphalytics-{graphalytics-version}-graphmat-{platform-version}.tar.gz`.



### Verify the necessary prerequisites
The software listed below are required by the GraphMat platform driver, which should be properly configured in the cluster environment. Softwares that are provided are already included in the platform driver.

| Software | Version (tested) | Usage | Description | Provided |
|-------------|:-------------:|:-------------:|:-------------:|:-------------:|
| GraphMat | 2.0[*](https://github.com/narayanan2004/GraphMat/) | Platform | Providing GraphMat implementation | - | - |
| Graphalytics | 1.0 | Driver| Graphalytics benchmark suite | ✔(maven) |
| Granula | 1.0 | Driver | Fine-grained performance analysis | ✔(maven) |
| Intel MPI | 5.1.2 | Deployment | Job deployment | - |
| Slurm | ? | Deployment | Job deployment | - |
| NFS | any | Deployment | Shared storage | - |
| JDK | 7+ | Build | Java virtual machine | - |
| Maven | 3.3.9 | Build | Building the platform driver | - |
| GNU Make | 4.0 | Build | Building Graphmat code | - |
| CMake | 3.2.2 | Build | Building Graphmat code | - |
| Intel Compiler | 15.0 | Build | Building Graphmat code | - |

 - `Intel Compiler`: Graphmat source code needs to be built with Intel Compiler.
 - `Slurm`: GraphMat platform driver needs to be deployed with Slurm.
 - `Intel MPI`: GraphMat platform driver needs to be deployed via Intel MPI.
 - `NFS`: GraphMat platform driver needs to be installed in a shared file system, e.g. NFS.

### Adjust the benchmark configurations
Adjust the GraphMat configurations in `config/platform.properties`.

 - `platform.graphmat.home`: Directory where GraphMat has been installed.
 - `platform.graphmat.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to GraphMat format.
 - `platform.graphmat.num-threads`: Number of threads to use when running GraphMat.
 - `platform.graphmat.command.convert`: The format of the command used to run the conversion executable. The default value is `%s %s` where the first argument refers to the binary name and the second argument refers to the binary arguments.
 - `platform.graphmat.command.run`: The format of the command used to run the bencharmk executables. The default value is `%s %s` where the first argument refers to the binary name and the second argument refers to the binary arguments.

### Running the benchmark

To execute a Graphalytics benchmark on Graphmat (using this driver), follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark).
