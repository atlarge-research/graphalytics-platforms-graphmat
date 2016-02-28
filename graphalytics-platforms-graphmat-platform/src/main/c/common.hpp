#include <cstring>
#include <fstream>
#include <ostream>
#include <string>
#include <sys/time.h>
#include <vector>

template <typename T>
void print_graph(const char *filename, const Graph<T>& graph) {
    if (filename == NULL || strlen(filename) == 0) {
        return;
    }

    std::ostream *stream;
    std::ofstream *file_stream;

    std::string mpi_filename = std::string("mpi-output-") + std::to_string(GraphPad::global_myrank);
    file_stream = new std::ofstream(mpi_filename.c_str());
    stream = file_stream;

    for (size_t i = 0; i < graph.nvertices; i++) {
        if (graph.vertexproperty.node_owner(i+1)) {
            (*stream) << i + 1 << " " << graph.getVertexproperty(i+1) << std::endl;
        }
    }

    if (file_stream != NULL) {
        if (!file_stream->good()) {
            std::cerr << "failed to write output to file" << endl;
        }

        file_stream->flush();
        file_stream->close();
        delete file_stream;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (GraphPad::global_myrank == 0) {
        std::ostream *stream_all;
        std::ofstream *file_stream_all;
        std::ifstream *mpi_stream;
        int mpi_comm_size;
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_comm_size);
        if (strcmp(filename, "-") != 0) {
            file_stream_all = new std::ofstream(filename);
            stream_all = file_stream_all;
        } else {
            file_stream_all = NULL;
            stream_all = &std::cout;
        }
        for (int i = 0; i < mpi_comm_size; i++) {
            char buffer[1024];
            mpi_filename = std::string("mpi-output-") + std::to_string(i);
            mpi_stream = new std::ifstream(mpi_filename.c_str());
            (*mpi_stream).getline(buffer, 1024);
            while (!(*mpi_stream).eof()) {
                (*stream_all) << buffer << std::endl;
                (*mpi_stream).getline(buffer, 1024);
            }
            mpi_stream->close();
            delete mpi_stream;
            std::remove(mpi_filename.c_str());
        }
        if (file_stream_all != NULL) {
            if (!file_stream_all->good()) {
                std::cerr << "failed to write output to file" << endl;
            }
            file_stream_all->flush();
            file_stream_all->close();
            delete file_stream_all;
        }
    }
}

bool get_bit(size_t idx, char* vec) {
    size_t offset = idx >> 3;
    size_t bit = idx & 0x7;
    char mask = 1 << bit;
    return (vec[offset] & mask) != 0;
}

bool set_bit(size_t idx, char* vec) {
    size_t offset = idx >> 3;
    size_t bit = idx & 0x7;
    char mask = 1 << bit;
    char val = vec[offset];

    if (!(val & mask)) {
        vec[offset] = val | mask;
        return true;
    }

    return false;
}

double timer() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

static std::vector<std::pair<std::string, double>> timers;

void timer_start() {
    timers.clear();
}

void timer_next(std::string name) {
    timers.push_back(std::make_pair(name, timer()));
}

void timer_end() {
    timer_next("end");

    std::cerr << "Timing results:" << std::endl;

    for (size_t i = 0; i < timers.size() - 1; i++) {
        std::string &name = timers[i].first;
        double time = timers[i + 1].second - timers[i].second;

        std::cerr << " - "  << name << ": " << time << " sec" <<  std::endl;
    }

    timers.clear();
}
