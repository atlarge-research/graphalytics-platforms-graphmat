#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>

#include "GraphMatRuntime.cpp"
#include "common.hpp"

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

struct vertex_value_type {
    public:
        int id;
        double clustering_coef;
        vector<int> neighbors;
        char *bitvector;

        vertex_value_type() {
            id = -1;
            clustering_coef = 0.0;
            neighbors.clear();
            bitvector = NULL;
        }

        bool operator!=(const vertex_value_type& other) const {
            return other.id != id;
        }

        friend ostream& operator<<(ostream& stream, const vertex_value_type &v) {
            stream << v.clustering_coef;
            return stream;
        }
};

typedef vector<int> collect_reduce_type;
typedef int collect_msg_type;

class CollectNeighborsProgram: public GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {
    public:
        const int num_neighbors_threshold = 1024;
        int bitvector_size;

        CollectNeighborsProgram(int nvertices) {
            order = ALL_EDGES;
            activity = ALL_VERTICES;
            bitvector_size = nvertices / sizeof(char) + 1; // +1 for safety
        }

        bool send_message(const vertex_value_type& vertex, collect_msg_type& msg) const {
            msg = vertex.id;
            return true;
        }

        void process_message(const collect_msg_type& msg, const int edge, const vertex_value_type& vertex, collect_reduce_type& result) const {
            result.clear();
            result.push_back(msg);
        }

        void reduce_function(collect_reduce_type& total, const collect_reduce_type& partial) const {
            total.insert(total.end(), partial.cbegin(), partial.cend());
        }

        void apply(const collect_reduce_type& total, vertex_value_type& vertex) {
            vertex.neighbors = total;
            vector<int> &vec = vertex.neighbors;

            if (num_neighbors_threshold > 1024) {
                vertex.bitvector = new char[bitvector_size];
                memset(vertex.bitvector, 0, bitvector_size);

                for (auto it = vec.begin(); it != vec.end(); it++) {
                    set_bit(*it, vertex.bitvector);
                }
            } else {
                sort(vec.begin(), vec.end());
                auto last = unique(vec.begin(), vec.end());
                vec.erase(last, vec.end());
            }
        }
};

typedef int count_reduce_type;
typedef const vertex_value_type* count_msg_type;

class CountTrianglesProgram: public GraphProgram<count_msg_type, count_reduce_type, vertex_value_type> {
    public:
        CountTrianglesProgram() {
            order = ALL_EDGES;
            activity = ALL_VERTICES;
        }

        bool send_message(const vertex_value_type& vertex, count_msg_type& msg) const {
            msg = &vertex;
            return true;
        }

        void process_message(const count_msg_type& msg, const int edge, const vertex_value_type& vertex, count_reduce_type& result) const {
            const vertex_value_type &a = vertex;
            const vertex_value_type &b = *msg;
            result = 0;

            if (a.bitvector == NULL && b.bitvector == NULL) {
                auto it_a = a.neighbors.begin();
                auto it_b = b.neighbors.begin();

                auto end_a = a.neighbors.end();
                auto end_b = b.neighbors.end();

                while (it_a != end_a && it_b != end_b) {
                    int delta = *it_a - *it_b;
                    if (delta == 0) result++;
                    if (delta <= 0) it_a++;
                    if (delta >= 0) it_b++;
                }

            } else {
                int pick_a = a.neighbors.size() < b.neighbors.size();
                auto p = pick_a ? a : b;
                auto q = pick_a ? b : a;

                for (auto it = p.neighbors.begin(); it != p.neighbors.end(); it++) {
                    result += get_bit(*it, q.bitvector);
                }
            }
        }

        void reduce_function(count_reduce_type& total, const count_reduce_type& partial) const {
            total += partial;
        }

        void apply(const count_reduce_type& total, vertex_value_type& vertex) {
            size_t deg = vertex.neighbors.size();
            size_t tri = total;
            vertex.clustering_coef = deg > 1 ? tri / (deg * (deg - 1.0)) : 0.0;
        }
};


int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "usage: " << argv[0] << " <graph file> [output file]" << endl;
        return EXIT_FAILURE;
    }

    char *filename = argv[1];
    char *output = argc > 2 ? argv[2] : NULL;

    nthreads = omp_get_max_threads();
    cout << "num. threads: " << nthreads << endl;

#ifdef GRANULA
    granula::operation graphmatJob("GraphMat", "Id.Unique", "Job", "Id.Unique");
    cout<<graphmatJob.getOperationInfo("StartTime", graphmatJob.getEpoch())<<endl;

    granula::operation loadGraph("GraphMat", "Id.Unique", "LoadGraph", "Id.Unique");
    cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

    timer_start();

    timer_next("load graph");
    Graph<vertex_value_type> graph;
    graph.ReadMTX(filename, nthreads * 4);

#ifdef GRANULA
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    timer_next("initialize engine");
    for (size_t i = 0; i < graph.nvertices; i++) {
        graph.vertexproperty[i].id = i;
        graph.vertexproperty[i].clustering_coef = 0.0;
    }

    graph.setAllActive();

    CollectNeighborsProgram col_prog(graph.nvertices);
    CountTrianglesProgram cnt_prog;

    auto col_ctx = graph_program_init(col_prog, graph);
    auto cnt_ctx = graph_program_init(cnt_prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("run algorithm 1 (collect neighbors)");
    run_graph_program(&col_prog, graph, 1, &col_ctx);

    timer_next("run algorithm 2 (count triangles)");
    graph.setAllActive();
    run_graph_program(&cnt_prog, graph, 1, &cnt_ctx);

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

#ifdef GRANULA
    granula::operation offloadGraph("GraphMat", "Id.Unique", "OffloadGraph", "Id.Unique");
    cout<<offloadGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("print output");
    print_graph(output, graph);

#ifdef GRANULA
    cout<<offloadGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("deinitialize engine");
    graph_program_clear(col_ctx);
    graph_program_clear(cnt_ctx);

    timer_end();

#ifdef GRANULA
    cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
#endif

    return EXIT_SUCCESS;
}
