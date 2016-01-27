#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>

#include "GraphMatRuntime.cpp"
#include "common.hpp"

using namespace std;

typedef int32_t component_type;
typedef component_type msg_type;
typedef component_type reduce_type;

struct vertex_value_type {
    public:
        component_type curr;
        component_type prev;

        vertex_value_type(component_type id) {
            curr = id;
            prev = -1;
        }

        vertex_value_type() {
            curr = -1;
            prev = -1;
        }

        bool operator!=(const vertex_value_type& other) const {
            return !(curr == other.curr && prev == other.prev);
        }

        friend ostream& operator<< (ostream& stream, const vertex_value_type &v) {
            stream << v.curr;
            return stream;
        }
};


class WeaklyConnectedComponents: public GraphProgram<msg_type, reduce_type, vertex_value_type> {
    public:
        WeaklyConnectedComponents() {
            order = ALL_EDGES;
            activity = ACTIVE_ONLY;
        }

        bool send_message(const vertex_value_type& vertex, msg_type& msg) const {
            msg = vertex.curr;
            return vertex.curr != vertex.prev;
        }

        void process_message(const msg_type& msg, const int edge, const vertex_value_type& vertex, reduce_type& result) const {
            result = msg;
        }

        void reduce_function(reduce_type& total, const reduce_type& partial) const {
            total = min(total, partial);
        }

        void apply(const reduce_type& total, vertex_value_type& vertex) {
            vertex.prev = vertex.prev;
            vertex.curr = min(vertex.curr, total);
        }
};


int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "usage: " << argv[0] << " <graph file> [output file]" << endl;
    }

    char *filename = argv[1];
    char *output = argc > 2 ? argv[2] : NULL;

    nthreads = omp_get_max_threads();
    cout << "num. threads: " << nthreads << endl;


    WeaklyConnectedComponents prog;
    Graph<vertex_value_type> graph;

    graph.ReadMTX(filename, nthreads * 4);

    for (size_t i = 0; i < graph.nvertices; i++) {
        graph.setVertexproperty(i, vertex_value_type(i));
    }

    graph.setAllActive();
    auto ctx = graph_program_init(prog, graph);

    double before = timer();
    run_graph_program(&prog, graph, -1, &ctx);
    cout << "run time: " << timer() - before << endl;

    graph_program_clear(ctx);
    print_graph(output, graph);
}
