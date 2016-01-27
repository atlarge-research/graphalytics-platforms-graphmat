#include <cstring>
#include <fstream>
#include <ostream>
#include <sys/time.h>

template <typename T>
void print_graph(const char *filename, const Graph<T>& graph) {
    if (filename == NULL || strlen(filename) == 0) {
        return;
    }

    std::ostream *stream;
    std::ofstream *file_stream;

    if (strcmp(filename, "-") != 0) {
        file_stream = new std::ofstream(filename);
        stream = file_stream;
    } else {
        file_stream = NULL;
        stream = &std::cout;
    }

    for (size_t i = 0; i < graph.nvertices; i++) {
        (*stream) << i + 1 << " " << graph.vertexproperty[i] << std::endl;
    }

    if (file_stream != NULL) {
        if (!file_stream->good()) {
            std::cerr << "failed to write output to file" << endl;
        }

        file_stream->flush();
        file_stream->close();
        delete file_stream;
    }
}

bool get_bit(size_t idx, char* vec) {
    size_t offset = idx >> 3;
    size_t bit = idx & 0x7;
    char mask = 1 << bit;
    return (vec[offset] & mask) != 0;
}

void set_bit(size_t idx, char* vec) {
    size_t offset = idx >> 3;
    size_t bit = idx & 0x7;
    char mask = 1 << bit;
    char val = vec[offset];

    if (!(val & mask)) {
        vec[offset] = val | mask;
    }
}

double timer() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
