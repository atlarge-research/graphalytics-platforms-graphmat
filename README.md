# Graphalytics GraphMat platform extension

## Note on support

Current development is happening in the [dataformat-granula-distr branch](https://github.com/tudelft-atlarge/graphalytics-platforms-graphmat/tree/dataformat-granula-distr). We are adapting the code to the latest [GraphMat](https://github.com/narayanan2004/GraphMat/tree/distributed_primitives_integration) and [Graphalytics](https://github.com/ldbc/ldbc_graphalytics/tree/dataformat-granula) APIs. However, the code is not yet stable; we hope to have a release in February 2016. Unfortunately, we do not have the resources to address issues in the older code in the master branch.

## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.

The following dependencies are required for this platform extension (in parentheses are the tested versions):

* Intel compiler (icpc)
* [GraphMat](https://github.com/narayanan2004/GraphMat/)
* CMake (3.2.2)
* GNU Make (4.0)

Download [GraphMat](https://github.com/narayanan2004/GraphMat/) and unpack into any directory. Modify `graphmat.home` in `config/graphmat.properties` to point to this directory or set the environment variable `GRAPHMAT_HOME` to this directory.

Finally, refer to the documation of the Graphayltics core on how to build and run this platform repository.


## GraphMat-specific benchmark configuration

Edit `config/graphmat.properties` to change the following settings:

- `graphmat.home`: Directory where GraphMat has been installed.
- `graphmat.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to GraphMat format.
- `graphmat.num-threads`: Number of threads to use when running GraphMat.
- `graphmat.command.convert`: The format of the command used to run the conversion executable. The default value is `%s %s` where the first argument refers to the binary name and the second argument refers to the binary arguments.
- `graphmat.command.run`: The format of the command used to run the bencharmk executables. The default value is `env KMP_AFFINITY=scatter numactl -i all %s %s` as recommended by the authors of GraphMat.

