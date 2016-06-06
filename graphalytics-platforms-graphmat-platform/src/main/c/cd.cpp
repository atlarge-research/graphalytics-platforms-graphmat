#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <unordered_map>

#include "GraphMatRuntime.cpp"
#include "common.hpp"

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

class histogram;

typedef int label_type;
typedef label_type msg_type;
typedef histogram reduce_type;
typedef label_type vertex_value_type;

class histogram {
    const int TYPE_EMPTY = 1;
    const int TYPE_ONE = 2;
    const int TYPE_MANY = 3;

    int type;
    union {
        unordered_map<label_type, int> *map;
        label_type first_label;
    } data;

    public:
        histogram() {
            type = TYPE_EMPTY;
        }

        void clear() {
            if (type == TYPE_EMPTY) {
                //
            } else if (type == TYPE_ONE) {
                type = TYPE_EMPTY;
            } else if (type == TYPE_MANY) {
                data.map->clear();
            }
        }

        void _upgrade() {
            if (type != TYPE_MANY) {
                int old_type = type;
                label_type old_label = data.first_label;

                type = TYPE_MANY;
                data.map = new unordered_map<label_type, int>();

                if (old_type != TYPE_EMPTY) {
                    add(old_label);
                }
            }
        }

        void add(const label_type label) {
            if (type == TYPE_EMPTY) {
                type = TYPE_ONE;
                data.first_label = label;
            } else {
                _upgrade();
                (*data.map)[label] += 1;
            }
        }

        void merge(const histogram& other) {
            if (other.type == TYPE_EMPTY) {
                // nothing
            } else if (other.type == TYPE_ONE) {
                add(other.data.first_label);
            } else {
                _upgrade();
                auto m = other.data.map;

                for (auto it = m->cbegin(); it != m->cend(); it++) {
                    (*data.map)[it->first] += it->second;
                }
            }
        }

        label_type most_common() const {
            if (type == TYPE_EMPTY) {
                return -1;
            } else if (type == TYPE_ONE) {
                return data.first_label;
            } else {
                auto m = data.map;

                label_type best_label = -1;
                int best_freq = 0;

                for (auto it = m->cbegin(); it != m->cend(); it++) {
                    label_type label = it->first;
                    int freq = it->second;

                    if (freq > best_freq || (freq == best_freq && label < best_label)) {
                        best_label = label;
                        best_freq = freq;
                    }
                }

                return best_label;
            }
        }

        histogram& operator=(const histogram &other) {
            clear();
            merge(other);
            return *this;
        }

        ~histogram() {
            if (type == TYPE_MANY) {
                delete data.map;
            }
        }
};

class CommunityDetectionProgram: public GraphProgram<msg_type, reduce_type, vertex_value_type> {
    public:
        CommunityDetectionProgram() {
            order = ALL_EDGES;
            activity = ALL_VERTICES;
        }

        bool send_message(const vertex_value_type& vertex, msg_type& msg) const {
            msg = vertex;
            return true;
        }

        void process_message(const msg_type& msg, const int edge, const vertex_value_type& vertex, reduce_type& result) const {
            result.clear();
            result.add(msg);
        }

        void reduce_function(reduce_type& total, const reduce_type& partial) const {
            total.merge(partial);
        }

        void apply(const reduce_type& total, vertex_value_type& vertex) {
            vertex = total.most_common();
        }
};


int main(int argc, char *argv[]) {

#ifdef GRANULA
    granula::startMonitorProcess(getpid());
#endif

    if (argc < 3) {
        cerr << "usage: " << argv[0] << " <graph file> <niterations> [output file]" << endl;
        return EXIT_FAILURE;;
    }

    char *filename = argv[1];
    int niterations = atoi(argv[2]);
    string jobId = argc > 3 ? argv[3] : NULL;
    char *output = argc > 4 ? argv[4] : NULL;

    nthreads = omp_get_max_threads();
    cout << "num. threads: " << nthreads << endl;

#ifdef GRANULA
    granula::linkNode(jobId);
    granula::linkProcess(getpid(), jobId);
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
    graph.setAllActive();

    for (size_t i = 0; i < graph.nvertices; i++) {
        graph.vertexproperty[i] = label_type(i);
    }

    CommunityDetectionProgram prog;
    auto ctx = graph_program_init(prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("run algorithm");
    run_graph_program(&prog, graph, niterations, &ctx);

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
    graph_program_clear(ctx);

    timer_end();

#ifdef GRANULA
    cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
        granula::stopMonitorProcess(getpid());
#endif


    return EXIT_SUCCESS;
}
