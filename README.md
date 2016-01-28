# Graphalytics GraphMat platform extension


## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.

The following dependencies are required for this platform extension (in parentheses are the tested versions):

* Intel compiler (icpc)
* [GraphMat](https://github.com/narayanan2004/GraphMat/)
* CMake (3.2.2)
* GNU Make (4.0)

Download [GraphMat](https://github.com/narayanan2004/GraphMat/), unpack into any directory and modify in `src/main/c/config.mk` to variable `GRAPHMAT_HOME` to point to the root directory where GraphMat is installed.

Finally, refer to the documation of the Graphayltics core on how to build and run this platform repository.


## GraphMat-specific benchmark configuration

Edit `config/powergraph.properties` to change the following settings:

- `graphmat.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to GraphMat format.
- `graphmat.num-threads`: Number of threads to use when running GraphMat.
- `graphmat.command.convert`: The format of the command used to run the conversion executable. The default value is "%s %s" where the first argument refers to the binary name and the second argument refers to the binary arguments.
- `graphmat.command.run`: The format of the command used to run the bencharmk executables. The default value is "env KMP_AFFINITY=scatter numactl -i all %s %s" as recommended by the authors of GraphMat.

