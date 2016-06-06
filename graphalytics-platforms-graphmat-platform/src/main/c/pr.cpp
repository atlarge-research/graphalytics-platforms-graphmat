#include "GraphMatRuntime.cpp"
#include "common.hpp"

#include <algorithm>
#include <atomic>
#include <iostream>
#include <limits>
#include <omp.h>
#include <stdint.h>

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

typedef double score_type;
typedef score_type msg_type;
typedef score_type reduce_type;

struct vertex_value_type {
    public:
        score_type score;
        int out_degree;
        int in_degree;

        vertex_value_type() {
            score = 0.0;
            out_degree = 0;
            in_degree = 0;
        }

        vertex_value_type(score_type score_out) {
            score = score_out;
        }

        bool operator!=(const vertex_value_type& other) const {
            return !(out_degree == other.out_degree && score == other.score);
        }

        friend ostream& operator<<(ostream& stream, const vertex_value_type &v) {
            stream << v.score;
            return stream;
        }

        score_type get_output() {
            return score;
        }
};

class OutDegreeProgram: public GraphProgram<int, int, vertex_value_type> {
    public:
        OutDegreeProgram() {
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

class InDegreeProgram: public GraphProgram<int, int, vertex_value_type> {
    public:
        InDegreeProgram() {
            order = OUT_EDGES;
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
            vertex.in_degree = total;
        }

};

void init_score_and_count_dangling(vertex_value_type* v, int* res, void* param_t) {
  int N = *(int*)param_t;
  v->score = 1.0/N;
  *res = (v->out_degree == 0)?(1):(0);
}

template <typename T>
void add(T a, T b, T *c, void* param_t) {
  *c = a+b;
}

struct param {
  double damping_factor;
  score_type dangling_sum;
  unsigned int N;
};


void update_zero_degree_vertices(vertex_value_type* v, score_type* res, void* param_t) {
  struct param* __param = (struct param*)param_t; 
  *res = 0;
  if (v->out_degree == 0) {
    *res = v->score;
  }
  if (v->in_degree == 0) {
    v->score = (1 - __param->damping_factor 
              + __param->damping_factor*__param->dangling_sum) / __param->N;
  }
}

class PageRankProgram: public GraphProgram<msg_type, reduce_type, vertex_value_type> {
    public:
        double damping_factor;
        score_type dangling_sum;
        Graph<vertex_value_type>& graph;

        PageRankProgram(Graph<vertex_value_type> &g, double df): graph(g), damping_factor(df) {
            order = OUT_EDGES;
            activity = ALL_VERTICES;
        }

        void init() {
            int ndangling = 0;

            /*#pragma omp parallel for reduction(+:ndangling)
            for (size_t i = 0; i < graph.nvertices; i++) {
                graph.vertexproperty[i].score = 1.0 / graph.nvertices;
                ndangling += (graph.vertexproperty[i].out_degree == 0);
            }*/
            int N = graph.getNumberOfVertices();
            graph.applyReduceAllVertices(&ndangling, init_score_and_count_dangling, add, (void*)&N);

            dangling_sum = double(ndangling) / graph.getNumberOfVertices();
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
            vertex.score = (1 - damping_factor) / graph.getNumberOfVertices()
                         + damping_factor * (total + dangling_sum / graph.getNumberOfVertices());
        }

        void do_every_iteration(int it) {
            //Fix vertices with 0 in and out degrees here.
            score_type next_dangling_sum = 0.0;

            /*#pragma omp parallel for reduction(+:next_dangling_sum)
            for (size_t i = 0; i < graph.nvertices; i++) {
                if (graph.vertexproperty[i].out_degree == 0) {
                    next_dangling_sum += graph.vertexproperty[i].score;
                }

                if (graph.vertexproperty[i].in_degree == 0) {
                    graph.vertexproperty[i].score = (1 - damping_factor +
                                                damping_factor*dangling_sum) / graph.nvertices;
                }
            }*/
            struct param param_t;
            param_t.damping_factor = damping_factor;
            param_t.dangling_sum = dangling_sum;
            param_t.N = graph.getNumberOfVertices();
            graph.applyReduceAllVertices(&next_dangling_sum, update_zero_degree_vertices, add, (void*)&param_t);

            dangling_sum = next_dangling_sum;
        }
};


int main(int argc, char *argv[]) {

#ifdef GRANULA
    granula::startMonitorProcess(getpid());
#endif

    MPI_Init(&argc, &argv);
    GraphPad::GB_Init();
    if (argc < 3) {
        cerr << "usage: " << argv[0] << " <graph file> <num iterations> [damping factor] [output file]" << endl;
        return EXIT_FAILURE;
    }




    bool is_master = GraphPad::global_myrank == 0;
    char *filename = argv[1];
    int niterations = atoi(argv[2]);
    double damping_factor = argc > 3 ? atof(argv[3]) : 0.85;
    string jobId = argc > 4 ? argv[4]: "DefaultJobId";
    char *output = argc > 5 ? argv[5] : NULL;

    nthreads = omp_get_max_threads();
    if (is_master) cout << "num. threads: " << nthreads << endl;



#ifdef GRANULA
    granula::linkNode(jobId);
    granula::linkProcess(getpid(), jobId);
    granula::operation graphmatJob("GraphMat", "Id.Unique", "Job", "Id.Unique");
    if (is_master) cout<<graphmatJob.getOperationInfo("StartTime", graphmatJob.getEpoch())<<endl;

    granula::operation loadGraph("GraphMat", "Id.Unique", "LoadGraph", "Id.Unique");
    if (is_master) cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

    timer_start(is_master);

    timer_next("load graph");
    Graph<vertex_value_type> graph;
    graph.ReadMTX(filename, nthreads * 4);

#ifdef GRANULA
    if (is_master) cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    timer_next("initialize engine");
    graph.setAllActive();

    OutDegreeProgram out_deg_prog;
    auto ctx1 = graph_program_init(out_deg_prog, graph);

    InDegreeProgram in_deg_prog;
    auto ctx2 = graph_program_init(in_deg_prog, graph);

    PageRankProgram pr_prog(graph, damping_factor);
    auto ctx3 = graph_program_init(pr_prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    if (is_master) cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("run algorithm 1 (count degree)");
    run_graph_program(&out_deg_prog, graph, 1);
    run_graph_program(&in_deg_prog, graph, 1);

    timer_next("initialize vertices");
    pr_prog.init();

    timer_next("run algorithm 2 (compute PageRank)");
    run_graph_program(&pr_prog, graph, niterations);

#ifdef GRANULA
    if (is_master) cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

#ifdef GRANULA
    granula::operation offloadGraph("GraphMat", "Id.Unique", "OffloadGraph", "Id.Unique");
    if (is_master) cout<<offloadGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("print output");
    print_graph<vertex_value_type, int, score_type>(output, graph, MPI_DOUBLE);

#ifdef GRANULA
    if (is_master) cout<<offloadGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("deinitialize engine");
    graph_program_clear(ctx1);
    graph_program_clear(ctx2);
    graph_program_clear(ctx3);

    timer_end();

#ifdef GRANULA
    if (is_master) cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
    granula::stopMonitorProcess(getpid());
#endif

    MPI_Finalize();
    return EXIT_SUCCESS;
}
