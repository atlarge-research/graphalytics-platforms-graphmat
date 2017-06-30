/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "GraphMatRuntime.h"
#include "common.hpp"

#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <chrono>

#ifdef GRANULA
#include "granula.hpp"
#endif

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

        component_type get_output() {
            return curr;
        }
};


class WeaklyConnectedComponents: public GraphMat::GraphProgram<msg_type, reduce_type, vertex_value_type> {
    public:
        WeaklyConnectedComponents() {
            order = GraphMat::ALL_EDGES;
            activity = GraphMat::ACTIVE_ONLY;
    	    process_message_requires_vertexprop = false;
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
            vertex.prev = vertex.curr;
            vertex.curr = min(vertex.curr, total);
        }
};

string getEpoch() {
    return to_string(chrono::duration_cast<chrono::milliseconds>
        (chrono::system_clock::now().time_since_epoch()).count());
}


int main(int argc, char *argv[]) {

#ifdef GRANULA
    granula::startMonitorProcess(getpid());
#endif

    MPI_Init(&argc, &argv);
    if (argc < 2) {
        cerr << "usage: " << argv[0] << " <graph file> [output file]" << endl;
        return EXIT_FAILURE;
    }

    bool is_master = GraphMat::get_global_myrank() == 0;
    char *filename = argv[1];
    string jobId = argc > 2 ? argv[2] : NULL;
    char *output = argc > 3 ? argv[3] : NULL;

    //nthreads = omp_get_max_threads();
    //if (is_master) cout << "num. threads: " << nthreads << endl;

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
    GraphMat::Graph<vertex_value_type> graph;
    //graph.ReadMTX(filename);
    graph.ReadGraphMatBin(filename);

#ifdef GRANULA
    if (is_master) cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    timer_next("initialize engine");
    for (size_t i = 1; i <= graph.nvertices; i++) {
        graph.setVertexproperty(i, vertex_value_type(i));
    }

    graph.setAllActive();

    WeaklyConnectedComponents prog;
    auto ctx = GraphMat::graph_program_init(prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    if (is_master) cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    if (is_master) cout<<" Processing starts at: " + getEpoch() + "\n" <<endl;
    timer_next("run algorithm");
    GraphMat::run_graph_program(&prog, graph, GraphMat::UNTIL_CONVERGENCE, &ctx);
    if (is_master) cout<< "Processing ends at: " + getEpoch() + "\n" <<endl;

#ifdef GRANULA
    if (is_master) cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

#ifdef GRANULA
    granula::operation offloadGraph("GraphMat", "Id.Unique", "OffloadGraph", "Id.Unique");
    if (is_master) cout<<offloadGraph.getOperationInfo("StartTime", offloadGraph.getEpoch())<<endl;
#endif

    timer_next("print output");
    print_graph<vertex_value_type, int, component_type>(output, graph, MPI_INT);

#ifdef GRANULA
    if (is_master) cout<<offloadGraph.getOperationInfo("EndTime", offloadGraph.getEpoch())<<endl;
#endif

    timer_next("deinitialize engine");
    GraphMat::graph_program_clear(ctx);

    timer_end();

#ifdef GRANULA
    if (is_master) cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
    granula::stopMonitorProcess(getpid());
#endif

    MPI_Finalize();
    return EXIT_SUCCESS;
}
