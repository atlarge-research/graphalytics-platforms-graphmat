#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

using namespace std;

int main(int argc, char *argv[]) {
    if (argc < 4) {
        cerr << "usage: " << argv[0] << " <input vertex file> <input edge file> <output text file>" << endl;
        return EXIT_FAILURE;
    }

    char *vertex_path = argv[1];
    char *edge_path = argv[2];
    char *output_path = argv[3];

    string line;

    unordered_map<uint64_t, uint64_t> new2old;
    unordered_map<uint64_t, uint64_t> old2new;
    vector<pair<uint64_t, uint64_t>> edges;
    uint64_t n = 0;

    // Read vertices from vertex file
    ifstream vertex_stream(vertex_path);
    if (!vertex_stream) {
        cerr << "failed to open: " << vertex_path << endl;
        return EXIT_FAILURE;
    }

    while (getline(vertex_stream, line)) {
        uint64_t v, u;
        istringstream iss(line);

        if (!(iss >> v)) {
            cerr << "error: invalid line in " << vertex_path << endl;
            return EXIT_FAILURE;
        }

        u = (n++) + 1; // GraphMat requires 1-based indices

        new2old[u] = v;
        old2new[v] = u;
    }

    vertex_stream.close();

    // Read edges from edge file
    ifstream edge_stream(edge_path);
    if (!edge_stream) {
        cerr << "failed to open: " << edge_path << endl;
        return EXIT_FAILURE;
    }

    while (getline(edge_stream, line)) {
        uint64_t v, u;
        istringstream iss(line);

        if (!(iss >> v >> u)) {
            cerr << "error: invalid line in " << edge_path << endl;
            return EXIT_FAILURE;
        }

        if (old2new.count(v) == 0 || old2new.count(u) == 0) {
            cerr << "error: edge (" << v << ", " << u << ") appears in edge file but "
                "vertices do not appear in vertex file" << endl;
            return EXIT_FAILURE;
        }

        edges.push_back(make_pair(old2new[v], old2new[u]));
    }

    // Write edges to output file
    ofstream output_stream(output_path, ios::in | ios::trunc);
    if (!output_stream) {
        cerr << "failed to open: " << output_path << endl;
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < edges.size(); i++) {
        output_stream << edges[i].first << " " << edges[i].second << endl;
    }

    output_stream.flush();
    output_stream.close();


    return EXIT_SUCCESS;
}
