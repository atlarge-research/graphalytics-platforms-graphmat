#include <cstring>
#include <fstream>
#include <ostream>
#include <string>
#include <sys/time.h>
#include <vector>

template <typename T, typename E=int, typename O>
void print_graph(const char *filename, const Graph<T, E>& graph, MPI_Datatype mpi_datatype) {
    if (filename == NULL || strlen(filename) == 0) {
        return;
    }
    std::ostream *stream;
    std::ofstream *file_stream;
    if (GraphPad::global_myrank == 0) {
        if (strcmp(filename, "-") != 0) {
            file_stream= new std::ofstream(filename);
            stream= file_stream;
        } else {
            file_stream= NULL;
            stream= &std::cout;
        }
    }
    int mpi_size;
    int *id_buf_send;
    O *property_buf_send;
    int *id_buf_recv;
    O *property_buf_recv;
    int *recv_counts;
    int *displs;
    int nodes = 0;
    int nodes_sum = 0;

    if (GraphPad::global_myrank == 0) {
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
        recv_counts = (int *)malloc(mpi_size * sizeof(int));
        displs = (int *)malloc(mpi_size * sizeof(int));
    }
    for (int i = 1; i <= graph.nvertices; i++) {
        if (graph.vertexproperty.node_owner(i)) {
            nodes++;
        }
    }
    id_buf_send = (int*)malloc(nodes * sizeof(int));
    property_buf_send = (O*)malloc(nodes * sizeof(O));

    MPI_Gather(&nodes, 1, MPI_INT, recv_counts, 1, MPI_INT, 0, MPI_COMM_WORLD); 
    if (GraphPad::global_myrank == 0) {
        for (int i = 0; i < mpi_size; i++) {
            displs[i] = nodes_sum;
            nodes_sum += recv_counts[i];
        }
        id_buf_recv = (int*)malloc(nodes_sum * sizeof(int));
        property_buf_recv = (O*)malloc(nodes_sum * sizeof(O));
    }
    nodes = 0;
    for (int i = 1; i <= graph.nvertices; i++) {
        if (graph.vertexproperty.node_owner(i)) {
            id_buf_send[nodes] = i;
            T vertex_property = graph.getVertexproperty(i);
            property_buf_send[nodes] = vertex_property.get_output();
            nodes++;
        }
    }
    MPI_Gatherv(id_buf_send, nodes, MPI_INT, id_buf_recv, recv_counts, displs, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gatherv(property_buf_send, nodes, mpi_datatype, property_buf_recv, recv_counts, displs, mpi_datatype, 0, MPI_COMM_WORLD);
    if (GraphPad::global_myrank == 0) {
        for (int i = 0; i < nodes_sum; i++) {
            (*stream) << id_buf_recv[i] << " " << T(property_buf_recv[i]) << std::endl;
        }

        free(recv_counts);
        free(displs);
        free(id_buf_recv);
        free(property_buf_recv);

        if (file_stream != NULL) {
            if (!file_stream->good()) {
                std::cerr << "failed to write output to file" << endl;
            }
    
            file_stream->flush();
            file_stream->close();
            delete file_stream;
        }

    }

    free(id_buf_send);
    free(property_buf_send);


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

static bool timer_enabled;
static std::vector<std::pair<std::string, double>> timers;

void timer_start(bool flag=true) {
    timer_enabled = flag;
    timers.clear();
}

void timer_next(std::string name) {
    if (timer_enabled) {
        timers.push_back(std::make_pair(name, timer()));
    }
}

void timer_end() {
    timer_next("end");

    if (timer_enabled) {
        std::cerr << "Timing results:" << std::endl;

        for (size_t i = 0; i < timers.size() - 1; i++) {
            std::string &name = timers[i].first;
            double time = timers[i + 1].second - timers[i].second;

            std::cerr << " - "  << name << ": " << time << " sec" <<  std::endl;
        }

        timers.clear();
    }
}
