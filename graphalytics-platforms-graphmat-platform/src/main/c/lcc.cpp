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
    int triangles;
    std::vector<int> all_neighbors;
    std::vector<int> out_neighbors;
    double clustering_coef;
    int* bitvector;

  public:
    vertex_value_type() {
      id = -1;
      triangles = 0;
      all_neighbors.clear();
      out_neighbors.clear();
      bitvector = NULL;
      clustering_coef = 0.0;
    }
    bool operator!=(const vertex_value_type& t) const {
      return (t.triangles != this->triangles); //dummy
    }
    ~vertex_value_type() {
      all_neighbors.clear();
      out_neighbors.clear();
      triangles = 0;
    }
    friend ostream& operator<<(ostream& stream, const vertex_value_type &v) {
            //stream << v.triangles << " "<< v.out_neighbors.size() << " " << v.all_neighbors.size() << " " << v.clustering_coef;
            stream << v.clustering_coef;
            return stream;
        }

};

typedef vector<int> collect_reduce_type;
typedef int collect_msg_type;


class CollectNeighborsOutProgram: public GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:
      int BVLENGTH;


  CollectNeighborsOutProgram(int maxvertices) {
    this->order = IN_EDGES;
    BVLENGTH = (maxvertices+31)/32 + 32; //32 for safety
  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
    assert(b.size() == 1);
    a.push_back(b[0]);
  }

  void process_message(const collect_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, collect_reduce_type& res) const {
    res.clear(); 
    res.push_back(message);
  }
  bool send_message(const vertex_value_type& vertexprop, collect_msg_type& message) const {
    message = vertexprop.id;
    return true;
  }
  void apply(const collect_reduce_type& message_out, vertex_value_type& vertexprop) {
    vertexprop.all_neighbors = message_out;
    vertexprop.out_neighbors = message_out;

    /*if (vertexprop.neighbors.size() > 1024) {
      vertexprop.bitvector = new int[BVLENGTH];
      for (auto it = vertexprop.neighbors.begin(); it != vertexprop.neighbors.end(); ++it) {
        set_bit(*it, (char*)vertexprop.bitvector);
      }
    } else {
      std::sort(vertexprop.neighbors.begin(), vertexprop.neighbors.end());
    }*/
  }

};
class CollectNeighborsInProgram: public GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:
      int BVLENGTH;


  CollectNeighborsInProgram(int maxvertices) {
    this->order = OUT_EDGES;
    BVLENGTH = (maxvertices+31)/32 + 32; //32 for safety
  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
    assert(b.size() == 1);
    a.push_back(b[0]);
  }

  void process_message(const collect_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, collect_reduce_type& res) const {
    res.clear(); 
    res.push_back(message);
  }
  bool send_message(const vertex_value_type& vertexprop, collect_msg_type& message) const {
    message = vertexprop.id;
    return true;
  }
  void apply(const collect_reduce_type& message_out, vertex_value_type& vertexprop) {
    std::sort(vertexprop.out_neighbors.begin(), vertexprop.out_neighbors.end());

    auto& all_neighbors = vertexprop.all_neighbors;
    all_neighbors.insert(all_neighbors.end(), message_out.begin(), message_out.end());

    std::sort(all_neighbors.begin(), all_neighbors.end());
    auto last = unique(all_neighbors.begin(), all_neighbors.end());
    all_neighbors.erase(last, all_neighbors.end());

    /*if (vertexprop.neighbors.size() > 1024) {
      vertexprop.bitvector = new int[BVLENGTH];
      for (auto it = vertexprop.neighbors.begin(); it != vertexprop.neighbors.end(); ++it) {
        set_bit(*it, (char*)vertexprop.bitvector);
      }
    } else {
      std::sort(vertexprop.neighbors.begin(), vertexprop.neighbors.end());
    }*/
  }

};

typedef int count_reduce_type;
typedef const vertex_value_type* count_msg_type;

class CountTrianglesProgram: public GraphProgram<count_msg_type, count_reduce_type, vertex_value_type> {

  public:
    int BVLENGTH;

  CountTrianglesProgram(int maxvertices) {
    this->order = ALL_EDGES;
    BVLENGTH = (maxvertices+31)/32 + 32; //32 for safety

  }

  void reduce_function(count_reduce_type& v, const count_reduce_type& w) const {
    v += w;
  }

  void process_message(const count_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, count_reduce_type& res) const {
    count_reduce_type tri = 0;
    const vertex_value_type& neighbor = *message;
    
    if (neighbor.bitvector == NULL && vertexprop.bitvector == NULL) {
      int it1 = 0, it2 = 0;
      int it1_end = neighbor.out_neighbors.size();
      int it2_end = vertexprop.all_neighbors.size();

      while (it1 != it1_end && it2 != it2_end){
        if (neighbor.out_neighbors[it1] == vertexprop.all_neighbors[it2]) {
          tri++;
          ++it1; ++it2;
        } else if (neighbor.out_neighbors[it1] < vertexprop.all_neighbors[it2]) {
          ++it1;
        } else {
          ++it2;
        }
      } 
      res = tri;
      return;

    } 

    /*else {
      int const* bv;
      std::vector<int>::const_iterator itb, ite;

      if (message.bitvector != NULL) { 
        bv = message.bitvector; 
        itb = vertexprop.neighbors.begin(); 
        ite = vertexprop.neighbors.end(); 
      } else { 
        bv = vertexprop.bitvector; 
        itb = message.neighbors.begin(); 
        ite = message.neighbors.end(); 
      }
      for (auto it = itb; it != ite; ++it) {
        res += get_bitvector(*it, bv);
      }
    }*/

  }

  bool send_message(const vertex_value_type& vertexprop, count_msg_type& message) const {
    message = &vertexprop;
    return true;
  }

  void apply(const count_reduce_type& message_out, vertex_value_type& vertexprop) {
    vertexprop.triangles = message_out;
    int deg = vertexprop.all_neighbors.size();
    vertexprop.clustering_coef = (deg > 1)?(2.0*(double)message_out/(double)(deg*(deg-1))):(0.0);
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
    Graph<vertex_value_type, int> graph;
    graph.ReadMTX(filename, nthreads * 4);

#ifdef GRANULA
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    timer_next("initialize engine");
    for (size_t i = 1; i <= graph.nvertices; i++) {
        vertex_value_type v;
        v.id = i;
        graph.setVertexproperty(i, v);
    }

    graph.setAllActive();

    CollectNeighborsOutProgram col_prog_out(graph.nvertices);
    CollectNeighborsInProgram col_prog_in(graph.nvertices);
    CountTrianglesProgram cnt_prog(graph.nvertices);

    auto col_ctx_out = graph_program_init(col_prog_out, graph);
    auto col_ctx_in = graph_program_init(col_prog_in, graph);
    auto cnt_ctx = graph_program_init(cnt_prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("run algorithm 1 - phase 1 & 2 (collect neighbors)");
    run_graph_program(&col_prog_out, graph, 1, &col_ctx_out);
    graph.setAllActive();
    run_graph_program(&col_prog_in, graph, 1, &col_ctx_in);

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
    graph_program_clear(col_ctx_out);
    graph_program_clear(col_ctx_in);
    graph_program_clear(cnt_ctx);

    timer_end();

#ifdef GRANULA
    cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
#endif

    return EXIT_SUCCESS;
}
