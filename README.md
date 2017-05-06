# Graphalytics GraphMat platform extension


## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.

The following dependencies are required for this platform extension (in parentheses are the tested versions):

* Intel compiler (icpc)
* [GraphMat](https://github.com/narayanan2004/GraphMat/)
* CMake (3.2.2)
* GNU Make (4.0)

Download [GraphMat](https://github.com/narayanan2004/GraphMat/) and unpack into any directory. Modify `platform.graphmat.home` in `config/graphmat.properties` to point to this directory or set the environment variable `GRAPHMAT_HOME` to this directory.

Finally, refer to the documation of the Graphayltics core on how to build and run this platform repository.


## GraphMat-specific benchmark configuration

Edit `config/graphmat.properties` to change the following settings:

- `platform.graphmat.home`: Directory where GraphMat has been installed.
- `platform.graphmat.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to GraphMat format.
- `platform.graphmat.num-threads`: Number of threads to use when running GraphMat.
- `platform.graphmat.command.convert`: The format of the command used to run the conversion executable. The default value is `%s %s` where the first argument refers to the binary name and the second argument refers to the binary arguments.
- `platform.graphmat.command.run`: The format of the command used to run the bencharmk executables. The default value is `env KMP_AFFINITY=scatter numactl -i all %s %s` as recommended by the authors of GraphMat.

