#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include "boost/serialization/vector.hpp"

#include "GraphMatRuntime.cpp"
#include "common.hpp"

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;


struct vertex_value_type : public GMDP::Serializable {
  public:
    int id;
    int triangles;
    std::vector<int> all_neighbors;
    std::vector<int> out_neighbors;
    double clustering_coef;
    //char* all_bitvector;
    //char* out_bitvector;
  public:
    vertex_value_type() {
      id = -1;
      triangles = 0;
      all_neighbors.clear();
      out_neighbors.clear();
      //all_bitvector = NULL;
      //out_bitvector = NULL;
      clustering_coef = 0.0;
    }

    vertex_value_type(double coef) {
	clustering_coef = coef;
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
            stream << v.clustering_coef;
            return stream;
        }

    double get_output() {
            return clustering_coef;
        }
    friend boost::serialization::access;
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
	ar & id;
	ar & triangles;
	ar & all_neighbors;
	ar & out_neighbors;
	ar & clustering_coef;
    }

};

template<typename T>
class serializable_vector : public GMDP::Serializable {
  public:
    std::vector<T> v;
  public:
    friend boost::serialization::access;
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & v;
    }
};
typedef serializable_vector<int> collect_reduce_type;
typedef int collect_msg_type;


class CollectNeighborsOutProgram: public GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:
      int bitvector_size;
      const int num_neighbors_threshold = 1024;


  CollectNeighborsOutProgram(int maxvertices) {
    order = IN_EDGES;
    activity = ALL_VERTICES;
    process_message_requires_vertexprop = false;
    bitvector_size = (maxvertices)/8 + 1; //for safety
  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
    //assert(b.size() == 1);
    //a.push_back(b[0]);
    a.v.insert(a.v.end(), b.v.begin(), b.v.end()); 
  }

  void process_message(const collect_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, collect_reduce_type& res) const {
    res.v.clear(); 
    res.v.push_back(message);
  }
  bool send_message(const vertex_value_type& vertexprop, collect_msg_type& message) const {
    message = vertexprop.id;
    return true;
  }
  void apply(const collect_reduce_type& message_out, vertex_value_type& vertexprop) {
    vertexprop.all_neighbors = message_out.v;
    vertexprop.out_neighbors = message_out.v;

    /*if (vertexprop.out_neighbors.size() > num_neighbors_threshold) {
      vertexprop.out_bitvector = new char[bitvector_size]();
      for (auto it = vertexprop.out_neighbors.begin(); it != vertexprop.out_neighbors.end(); ++it) {
        set_bit(*it, vertexprop.out_bitvector);
      }
    } else {*/
      std::sort(vertexprop.out_neighbors.begin(), vertexprop.out_neighbors.end());
    //}

  }

};
class CollectNeighborsInProgram: public GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:
      int bitvector_size;
      const int num_neighbors_threshold = 1024;


  CollectNeighborsInProgram(int maxvertices) {
    order = OUT_EDGES;
    activity = ALL_VERTICES;
    process_message_requires_vertexprop = false;
    bitvector_size = (maxvertices)/8 + 1; //for safety

  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
    //assert(b.size() == 1);
    //a.push_back(b[0]);
    a.v.insert(a.v.end(), b.v.begin(), b.v.end()); 
  }

  void process_message(const collect_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, collect_reduce_type& res) const {
    res.v.clear(); 
    res.v.push_back(message);
  }
  bool send_message(const vertex_value_type& vertexprop, collect_msg_type& message) const {
    message = vertexprop.id;
    return true;
  }
  void apply(const collect_reduce_type& message_out, vertex_value_type& vertexprop) {
    auto& all_neighbors = vertexprop.all_neighbors;
    all_neighbors.insert(all_neighbors.end(), message_out.v.begin(), message_out.v.end());

    /*if (vertexprop.all_neighbors.size() > num_neighbors_threshold) {
      vertexprop.all_bitvector = new char[bitvector_size]();
      for (auto it = vertexprop.all_neighbors.begin(); it != vertexprop.all_neighbors.end(); ++it) {
        set_bit(*it, vertexprop.all_bitvector);
      }
    } else {*/
      std::sort(all_neighbors.begin(), all_neighbors.end());
      auto last = unique(all_neighbors.begin(), all_neighbors.end());
      all_neighbors.erase(last, all_neighbors.end());
    //}

  }

};

typedef int count_reduce_type;
//typedef const vertex_value_type* count_msg_type;
typedef vertex_value_type count_msg_type;

class CountTrianglesProgram: public GraphProgram<count_msg_type, count_reduce_type, vertex_value_type> {

  public:

  CountTrianglesProgram() {
    order = ALL_EDGES;
    activity = ALL_VERTICES;
  }

  void reduce_function(count_reduce_type& v, const count_reduce_type& w) const {
    v += w;
  }

  void process_message(const count_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, count_reduce_type& res) const {
    count_reduce_type tri = 0;
    //const vertex_value_type& neighbor = *message;
    const vertex_value_type& neighbor(message);
    
    /*char* bv;
    std::vector<int>::const_iterator itb, ite;
    if (vertexprop.all_bitvector != NULL) {
      bv = vertexprop.all_bitvector;
      itb = neighbor.out_neighbors.begin(); 
      ite = neighbor.out_neighbors.end(); 
      for (auto it = itb; it != ite; ++it) {
        tri += get_bit(*it, bv);
      }
    } else if (neighbor.out_bitvector != NULL) {
      bv = neighbor.out_bitvector;
      itb = vertexprop.all_neighbors.begin(); 
      ite = vertexprop.all_neighbors.end(); 
      for (auto it = itb; it != ite; ++it) {
        tri += get_bit(*it, bv);
      }
    } else {*/
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
    //} 

    res = tri;
    return;
  }

  bool send_message(const vertex_value_type& vertexprop, count_msg_type& message) const {
    message = vertexprop;
    return true;
  }

  void apply(const count_reduce_type& message_out, vertex_value_type& vertexprop) {
    vertexprop.triangles = message_out;
    int deg = vertexprop.all_neighbors.size();
    //vertexprop.clustering_coef = (deg > 1)?(2.0*(double)message_out/(double)(deg*(deg-1))):(0.0);
    vertexprop.clustering_coef = (deg > 1)?(0.5*(double)message_out/(double)(deg*(deg-1))):(0.0);
  }

};


int main(int argc, char *argv[]) {
    if (argc < 3) {
        cerr << "usage: " << argv[0] << " <graph file> <isDirected> [output file]" << endl;
        return EXIT_FAILURE;
    }

    MPI_Init(&argc, &argv);

    char *filename = argv[1];
    int isDirected = atoi(argv[2]);
    char *output = argc > 3 ? argv[3] : NULL;

    int nthreads = omp_get_max_threads();
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
    graph.ReadMTX(filename);

#ifdef GRANULA
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    timer_next("initialize engine");
    for (size_t i = 1; i <= graph.getNumberOfVertices(); i++) {
	if (graph.vertexNodeOwner(i)) {
        	vertex_value_type v;
        	v.id = i;
        	graph.setVertexproperty(i, v);
	}
    }


    CollectNeighborsOutProgram col_prog_out(graph.nvertices);
    CollectNeighborsInProgram col_prog_in(graph.nvertices);
    CountTrianglesProgram cnt_prog;

    auto col_ctx_out = graph_program_init(col_prog_out, graph);
    auto col_ctx_in = graph_program_init(col_prog_in, graph);
    auto cnt_ctx = graph_program_init(cnt_prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("run algorithm 1 - phase 1 & 2 (collect neighbors)");
    run_graph_program(&col_prog_out, graph, 1, &col_ctx_out);
    run_graph_program(&col_prog_in, graph, 1, &col_ctx_in);

    timer_next("run algorithm 2 (count triangles)");
    run_graph_program(&cnt_prog, graph, 1, &cnt_ctx);

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

#ifdef GRANULA
    granula::operation offloadGraph("GraphMat", "Id.Unique", "OffloadGraph", "Id.Unique");
    cout<<offloadGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("print output");
    //print_graph(output, graph);
    print_graph<vertex_value_type, int, double>(output, graph, MPI_DOUBLE);

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
    MPI_Finalize();
    return EXIT_SUCCESS;
}
