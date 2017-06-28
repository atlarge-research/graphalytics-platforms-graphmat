# Graphalytics Graphmat platform driver

[![Build Status](http://jenkins.tribler.org/buildStatus/icon?job=Graphalytics_Graphmat_master_tester)](http://jenkins.tribler.org/job/Graphalytics_Graphmat_master_tester/) (TODO: setup Jenkins continuous integration) 

[GraphMat](https://github.com/narayanan2004/GraphMat) is ... (TODO: provide a short description on GraphMat).

To execute Graphalytics benchmark on Graphmat, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the GraphMat-specific instructions listed below.

### Obtain the platform driver
There are two possible ways to obtain the GraphMat platform driver:

 1. **Download the (prebuilt) [GraphMat platform driver](http://graphalytics.site/dist/stable/graphmat/) distribution from our website.

 2. **Build the platform drivers**: 
  - Download the source code from this repository.
  - Execute `mvn clean package` in the root directory (See details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
  - Extract the distribution from  `graphalytics-{graphalytics-version}-graphmat-{platform-version}.tar.gz`.



### Verify the necessary prerequisites
The softwares listed below are required by the GraphMat platform driver, which should be properly configured in the cluster environment. Softwares that are provided are already included in the platform driver.

| Software | Version (tested) | Usage | Description | Provided |
|-------------|:-------------:|:-------------:|:-------------:|:-------------:|
| GraphMat | [?](https://github.com/narayanan2004/GraphMat/) | Platform | Providing GraphMat implementation | - | - |
| Graphalytics | 1.0 (TODO) | Driver| Graphalytics benchmark suite | ✔(maven) |
| Granula | 0.1 (TODO) | Driver | Fine-grained performance analysis | ✔(maven) |
| Intel MPI | ? | Deployment | Job deployment | - |
| Slurm | ? | Deployment | Job deployment | - |
| NFS | any | Deployment | Shared storage | - |
| JDK | 7+ | Build | Java virtual machine | - |
| Maven | 3.3.9 | Build | Building the platform driver | - |
| GNU Make | 4.0 | Build | Building Graphmat code | - |
| CMake | 3.2.2 | Build | Building Graphmat code | - |
| Intel Compiler | ? | Build | Building Graphmat code | - |

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
