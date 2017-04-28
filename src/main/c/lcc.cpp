#include <limits>
#include <omp.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <chrono>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/utility.hpp"

#include "GraphMatRuntime.h"
#include "common.hpp"

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;


struct vertex_value_type : public GraphMat::Serializable {
  public:
    int id;
    std::vector<int> all_neighbors;
    std::vector<int> out_neighbors;
    double clustering_coef;
  public:
    vertex_value_type() {
      id = -1;
      all_neighbors.clear();
      out_neighbors.clear();
      clustering_coef = 0.0;
    }

    vertex_value_type(double coef) {
	clustering_coef = coef;
    }
    bool operator!=(const vertex_value_type& t) const {
      return (true); //dummy
    }
    ~vertex_value_type() {
      all_neighbors.clear();
      out_neighbors.clear();
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
	ar & all_neighbors;
	ar & out_neighbors;
	ar & clustering_coef;
    }

};

template<typename T>
class serializable_vector : public GraphMat::Serializable {
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


class CollectNeighborsOutProgram: public GraphMat::GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:
      bool isDirected;


  CollectNeighborsOutProgram(int maxvertices, bool _isDirected) {
    isDirected = _isDirected;
    order = GraphMat::IN_EDGES;
    activity = GraphMat::ALL_VERTICES;
    process_message_requires_vertexprop = false;
  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
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
    if (isDirected) {
      vertexprop.all_neighbors = message_out.v;
      vertexprop.out_neighbors = message_out.v;
      std::sort(vertexprop.out_neighbors.begin(), vertexprop.out_neighbors.end());
      std::sort(vertexprop.all_neighbors.begin(), vertexprop.all_neighbors.end());
    } else {
      vertexprop.all_neighbors = message_out.v;
      std::sort(vertexprop.all_neighbors.begin(), vertexprop.all_neighbors.end());
    }
  }

};
class CollectNeighborsInProgram: public GraphMat::GraphProgram<collect_msg_type, collect_reduce_type, vertex_value_type> {

  public:


  CollectNeighborsInProgram(int maxvertices) {
    order = GraphMat::OUT_EDGES;
    activity = GraphMat::ALL_VERTICES;
    process_message_requires_vertexprop = false;
  }

  void reduce_function(collect_reduce_type& a, const collect_reduce_type& b) const {
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

    std::sort(all_neighbors.begin(), all_neighbors.end());
    auto last = unique(all_neighbors.begin(), all_neighbors.end());
    all_neighbors.erase(last, all_neighbors.end());

  }

};

class count_msg_type : public GraphMat::Serializable {
  public:
    std::vector<int> v;
    int id;

    friend boost::serialization::access;
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & id;
      ar & v;
    }
};
typedef serializable_vector<pair< int, int> > count_reduce_type;

class CountTrianglesProgram: public GraphMat::GraphProgram<count_msg_type, count_reduce_type, vertex_value_type> {

  public:

  CountTrianglesProgram() {
    order = GraphMat::ALL_EDGES;
    activity = GraphMat::ALL_VERTICES;
  }

  void reduce_function(count_reduce_type& a, const count_reduce_type& b) const {
    a.v.insert(a.v.end(), b.v.begin(), b.v.end()); 
  }

  void process_message(const count_msg_type& message, const int edge_val, const vertex_value_type& vertexprop, count_reduce_type& res) const {
    int tri = 0;
      int it1 = 0, it2 = 0;
      const auto* neighbor_vector = &(message.v);
      int it1_end = neighbor_vector->size();
      int it2_end = vertexprop.all_neighbors.size();

      while (it1 != it1_end && it2 != it2_end){
        if (neighbor_vector->data()[it1] == vertexprop.all_neighbors[it2]) {
          tri++;
          ++it1; ++it2;
        } else if (neighbor_vector->data()[it1] < vertexprop.all_neighbors[it2]) {
          ++it1;
        } else {
          ++it2;
        }
      } 

    int id = message.id;
    auto x = make_pair(id, tri);
    res.v.clear(); 
    res.v.push_back(x); 

    return;
  }

  bool send_message(const vertex_value_type& vertex, count_msg_type& message) const {
    message.v = vertex.out_neighbors;
    message.id = vertex.id;
    return true;
  }

  void apply(const count_reduce_type& message_out, vertex_value_type& vertexprop) {
    auto v = message_out.v;
    std::sort(v.begin(), v.end());
    auto last = unique(v.begin(), v.end());
    
    int sum_of_elems = 0;
    std::for_each(v.begin(), last, [&] (pair<int, int> n) {
      sum_of_elems += n.second;
    });
    int deg = vertexprop.all_neighbors.size();
    vertexprop.clustering_coef = (deg > 1)?((double)(sum_of_elems)/(double)(deg)/(double)(deg-1)):(0.0);
  }

};

typedef int count_reduce_undirected_type;
typedef serializable_vector<int> count_msg_undirected_type;
class CountTrianglesUndirectedProgram: public GraphMat::GraphProgram<count_msg_undirected_type, count_reduce_undirected_type, vertex_value_type> {

  public:

  CountTrianglesUndirectedProgram() {
    order = GraphMat::OUT_EDGES;
    activity = GraphMat::ALL_VERTICES;
  }

  void reduce_function(count_reduce_undirected_type& a, const count_reduce_undirected_type& b) const {
    a += b;
  }

  void process_message(const count_msg_undirected_type& message, const int edge_val, const vertex_value_type& vertexprop, count_reduce_undirected_type& res) const {
    count_reduce_undirected_type tri = 0;
    
      int it1 = 0, it2 = 0;
      const auto* neighbor_vector = &(message.v);
      int it1_end = neighbor_vector->size();
      int it2_end = vertexprop.all_neighbors.size();

      while (it1 != it1_end && it2 != it2_end){
        if (neighbor_vector->data()[it1] == vertexprop.all_neighbors[it2]) {
          tri++;
          ++it1; ++it2;
        } else if (neighbor_vector->data()[it1] < vertexprop.all_neighbors[it2]) {
          ++it1;
        } else {
          ++it2;
        }
      } 

    res = tri;

    return;
  }

  bool send_message(const vertex_value_type& vertex, count_msg_undirected_type& message) const {
    message.v = vertex.all_neighbors;
    return true;
  }

  void apply(const count_reduce_undirected_type& message_out, vertex_value_type& vertexprop) {
    int deg = vertexprop.all_neighbors.size();
    vertexprop.clustering_coef = (deg > 1)?((double)(message_out)/(double)(deg)/(double)(deg-1)):(0.0);
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

    if (argc < 4) {
        cerr << "usage: " << argv[0] << " <graph file> <job id> <isDirected> [output file]" << endl;
        return EXIT_FAILURE;
    }

    MPI_Init(&argc, &argv);


    bool is_master = GraphMat::get_global_myrank() == 0;
    char *filename = argv[1];
	string jobId = argc > 2 ? argv[2] : "DefaultJobId";
    int isDirected = argc > 3 ? atoi(argv[3]) : NULL;
    char *output = argc > 4 ? argv[4] : NULL;

    int nthreads = omp_get_max_threads();
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
    GraphMat::Graph<vertex_value_type, int> graph;
    //graph.ReadMTX(filename);
    graph.ReadGraphMatBin(filename);

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


    CollectNeighborsOutProgram col_prog_out(graph.nvertices, isDirected);
    CollectNeighborsInProgram col_prog_in(graph.nvertices);

    CountTrianglesProgram cnt_prog;
    CountTrianglesUndirectedProgram cnt_undir_prog;

    auto col_ctx_out = GraphMat::graph_program_init(col_prog_out, graph);
    auto col_ctx_in = GraphMat::graph_program_init(col_prog_in, graph);
    auto cnt_ctx = GraphMat::graph_program_init(cnt_prog, graph);
    auto cnt_undir_ctx = GraphMat::graph_program_init(cnt_undir_prog, graph);

#ifdef GRANULA
    granula::operation processGraph("GraphMat", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    if (is_master) cout<<"Processing starts at: "<<getEpoch()<<endl;
    timer_next("run algorithm 1 - phase 1 & 2 (collect neighbors)");
    GraphMat::run_graph_program(&col_prog_out, graph, 1, &col_ctx_out);
    if(isDirected) GraphMat::run_graph_program(&col_prog_in, graph, 1, &col_ctx_in);

    timer_next("run algorithm 2 (count triangles)");
    if (isDirected) {
      GraphMat::run_graph_program(&cnt_prog, graph, 1, &cnt_ctx);
    } else {
      GraphMat::run_graph_program(&cnt_undir_prog, graph, 1, &cnt_undir_ctx);
    }
    if (is_master) cout<<"Processing ends at: "<<getEpoch()<<endl;

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
  /*MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 1; i <= graph.getNumberOfVertices(); i++) { 
    if (graph.vertexNodeOwner(i)) {
      printf("%d %f %d\n", i-1, graph.getVertexproperty(i).clustering_coef, graph.getVertexproperty(i).all_neighbors.size());
    }
    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);
  }*/
#ifdef GRANULA
    cout<<offloadGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
#endif

    timer_next("deinitialize engine");
    GraphMat::graph_program_clear(col_ctx_out);
    GraphMat::graph_program_clear(col_ctx_in);
    GraphMat::graph_program_clear(cnt_ctx);
    GraphMat::graph_program_clear(cnt_undir_ctx);

    timer_end();

#ifdef GRANULA
    cout<<graphmatJob.getOperationInfo("EndTime", graphmatJob.getEpoch())<<endl;
    granula::stopMonitorProcess(getpid());
#endif
    MPI_Finalize();
    return EXIT_SUCCESS;
}
