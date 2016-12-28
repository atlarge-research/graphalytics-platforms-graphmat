#include <cstring>
#include <fstream>
#include <ostream>
#include <string>
#include <sys/time.h>
#include <vector>

template <typename T, typename E>
void print_graph(const char *filename, const Graph<T, E>& graph) {
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

    for (size_t i = 1; i <= graph.nvertices; i++) {
        (*stream) << i << " " << graph.getVertexproperty(i) << std::endl;
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
