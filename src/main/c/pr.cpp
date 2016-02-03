#include <algorithm>
#include <atomic>
#include <iostream>
#include <limits>
#include <omp.h>
#include <stdint.h>

#include "GraphMatRuntime.cpp"
#include "common.hpp"

using namespace std;

typedef double score_type;
typedef score_type msg_type;
typedef score_type reduce_type;

struct vertex_value_type {
    public:
        score_type score;
        int out_degree;

        vertex_value_type() {
            score = 0.0;
            out_degree = 0;
        }

        bool operator!=(const vertex_value_type& other) const {
            return !(out_degree == other.out_degree && score == other.score);
        }

        friend ostream& operator<<(ostream& stream, const vertex_value_type &v) {
            stream << v.score;
            return stream;
        }
};

class DegreeProgram: public GraphProgram<int, int, vertex_value_type> {
    public:
        DegreeProgram() {
            order = IN_EDGES;
            activity = ALL_VERTICES;
        }

        bool send_message(const vertex_value_type& vertex, int& msg) const {
            msg = 1;
            return true;
        }

        void process_message(const int& msg, const int edge, const vertex_value_type& vertex, int& result) const {
            result = msg;
        }

        void reduce_function(int& total, const int& partial) const {
            total += partial;
        }

        void apply(const int& total, vertex_value_type& vertex) {
            vertex.out_degree = total;
        }

};


class PageRankProgram: public GraphProgram<msg_type, reduce_type, vertex_value_type> {
    public:
        double damping_factor;
        int nvertices;
        score_type dangling_sum;
        atomic<score_type> next_dangling_sum;

        PageRankProgram(double damping_factor, int nvertices) {
            order = OUT_EDGES;
            activity = ALL_VERTICES;
            this->damping_factor = damping_factor;
            this->nvertices = nvertices;
        }

        void set_dangling(int ndangling) {
            this->dangling_sum = ndangling / score_type(nvertices);
        }

        bool send_message(const vertex_value_type& vertex, msg_type& msg) const {
            msg = vertex.out_degree > 0 ? vertex.score / vertex.out_degree : 0.0;
            return true;
        }

        void process_message(const msg_type& msg, const int edge, const vertex_value_type& vertex, reduce_type& result) const {
            result = msg;
        }

        void reduce_function(reduce_type& total, const reduce_type& partial) const {
            total += partial;
        }

        void apply(const reduce_type& total, vertex_value_type& vertex) {
            vertex.score = (1 - damping_factor) / nvertices
                         + damping_factor * (total + dangling_sum / nvertices);

            if (vertex.out_degree == 0) {
                score_type a, b;

                do {
                    a = next_dangling_sum.load();
                    b = a + vertex.score;
                } while(!next_dangling_sum.compare_exchange_weak(a, b));
            }
        }

        void do_every_iteration(int it) {
            dangling_sum = next_dangling_sum.load();
            next_dangling_sum.store(0.0);
        }
};


int main(int argc, char *argv[]) {
    if (argc < 3) {
        cerr << "usage: " << argv[0] << " <graph file> <num iterations> [damping factor] [output file]" << endl;
        return EXIT_FAILURE;
    }

    char *filename = argv[1];
    int niterations = atoi(argv[2]);
    double damping_factor = argc > 3 ? atof(argv[3]) : 0.85;
    char *output = argc > 4 ? argv[4] : NULL;

    nthreads = omp_get_max_threads();
    cout << "num. threads: " << nthreads << endl;

    timer_start();

    timer_next("load graph");
    Graph<vertex_value_type> graph;
    graph.ReadMTX(filename, nthreads * 4);

    timer_next("initialize engine");
    graph.setAllActive();

    DegreeProgram deg_prog;
    auto ctx = graph_program_init(deg_prog, graph);

    PageRankProgram pr_prog(damping_factor, graph.nvertices);
    auto ctx2 = graph_program_init(pr_prog, graph);

    timer_next("run algorithm 1 (count degree)");
    run_graph_program(&deg_prog, graph, 1, &ctx);

    timer_next("run algorithm 2 (compute PageRank)");
    int ndangling = 0;

    for (size_t i = 0; i < graph.nvertices; i++) {
        graph.vertexproperty[i].score = 1.0 / graph.nvertices;
        ndangling += (graph.vertexproperty[i].out_degree == 0);
    }

    pr_prog.set_dangling(ndangling);
    run_graph_program(&pr_prog, graph, niterations, &ctx2);

    timer_next("print output");
    print_graph(output, graph);

    timer_next("deinitialize engine");
    graph_program_clear(ctx);
    graph_program_clear(ctx2);

    timer_end();

    return EXIT_SUCCESS;
}
