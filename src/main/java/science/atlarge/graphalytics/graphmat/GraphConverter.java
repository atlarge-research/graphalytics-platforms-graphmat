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
package science.atlarge.graphalytics.graphmat;

import it.unimi.dsi.fastutil.longs.*;
import science.atlarge.graphalytics.domain.graph.Graph;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

/**
 * Utility class for converting graphs in Graphalytics' VE format to the format supported by GraphMat
 */
public final class GraphConverter {

	private static final Logger LOG = LogManager.getLogger(GraphConverter.class);
	private static final long INVALID_ID = -1;

	
	/**
	 * Parses a graph in Graphalytics' VE format, writes the graph to a new file, and returns
	 * a mapping of vertex ids from the original graph to vertex ids in the output graph.
	 *
	 * @param vertexFile   the path of the vertex list for the input graph
	 * @param edgeFile     the path of the edge list for the input graph
	 * @param outputFile   the path to write the graph in GraphMath format to
	 * @param nvertices    the number of vertices in the graph
	 * @return a mapping of vertex ids from the original graph to vertex ids in the output graph
	 * @throws IOException iff an exception occurred while parsing the input graph or writing the output graph
	 */
	public static Long2LongMap parseAndWrite(String vertexFile, String edgeFile,
			String outputFile, long nvertices) throws IOException {
		LOG.debug(" - Reading vertex file " + vertexFile + " to construct ID translation");
		
		Long2LongMap old2new = new Long2LongOpenHashMap((int) nvertices);
		old2new.defaultReturnValue(INVALID_ID);
		
		try (BufferedReader r = new BufferedReader(new FileReader(vertexFile))) {
			long nextNewId = 1;
			String line;
			
			while ((line = r.readLine()) != null) {
				long oldId = Long.parseLong(line);
				long newId = nextNewId++;
				old2new.put(oldId, newId);
			}
		}
		
		LOG.debug(" - Converting edge file from " + edgeFile + " to " + outputFile);
		try (BufferedReader r = new BufferedReader(new FileReader(edgeFile));
				PrintWriter w = new PrintWriter(new FileWriter(outputFile))) {
			String line;
			
			while ((line  = r.readLine()) != null) {
				String[] parts = line.split(" ");
				
				if (parts.length < 2) {
					throw new IOException("Invalid line found in " + edgeFile);
				}
				
				long oldSrc = Long.parseLong(parts[0]);
				long oldDst = Long.parseLong(parts[1]);
				
				long newSrc = old2new.get(oldSrc);
				long newDst = old2new.get(oldDst);
				
				if (newSrc == INVALID_ID || newDst == INVALID_ID) {
					throw new IOException("Edge (" + oldSrc + "," + oldDst + ") is invalid since vertex ids are unknown");
				}

				w.print(newSrc + " " + newDst);
				for (int i = 2; i < parts.length; i++) {
					w.print(" " + parts[i]);
				}
				w.print("\n");
			}
		}
		
		return old2new;
	}
	
	public static Long2LongMap parseAndWrite(Graph g, String outputFile) throws IOException {
		return parseAndWrite(g.getVertexFilePath(), g.getEdgeFilePath(), outputFile, g.getNumberOfVertices());
	}
}
