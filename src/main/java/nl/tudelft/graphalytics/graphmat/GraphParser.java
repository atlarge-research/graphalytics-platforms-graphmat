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
package nl.tudelft.graphalytics.graphmat;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.MarkerManager;

import java.io.*;
import java.nio.file.Paths;

/**
 * Utility class for converting graphs in Graphalytics' VE format to the adjacency list format supported by GraphMat.
 * In addition, vertex IDs are normalized to range [0, number of vertices) to ensure compatibility with GraphMat.
 */
public final class GraphParser {


	private static final Logger LOG = LogManager.getLogger();

	private final String vertexFilePath;
	private final String edgeFilePath;
	private final String outputPath;
	private final boolean isGraphDirected;
	private final int numberOfVertices;
	private final String graphmatParserPath;
	private final String intermediatePath;

	private GraphParser(String vertexFilePath, String edgeFilePath,  String intermediatePath, String outputPath, boolean isGraphDirected,
						int numberOfVertices, String graphmatParserPath) {
		this.vertexFilePath = vertexFilePath;
		this.edgeFilePath = edgeFilePath;
		this.intermediatePath = intermediatePath;
		this.outputPath = outputPath;
		this.isGraphDirected = isGraphDirected;
		this.numberOfVertices = numberOfVertices;
		this.graphmatParserPath = graphmatParserPath;
	}

	@Override
	public String toString() {
		return "GraphParser{" +
				"vertexFilePath='" + vertexFilePath + '\'' +
				", edgeFilePath='" + edgeFilePath + '\'' +
				", outputPath='" + outputPath + '\'' +
				", isGraphDirected=" + isGraphDirected +
				", numberOfVertices=" + numberOfVertices +
				", graphmatParserPath='" + graphmatParserPath + '\'' +
				", intermediatePath='" + intermediatePath + '\'' +
				'}';
	}

	private void parseAndWrite() throws IOException {

		long maxVertexId = getMaxVertexId();
		parseEdgeList2EdgeValueList();
		parseEdgeValueList2Matrix(maxVertexId);
	}

	private long getMaxVertexId() throws IOException {
		LOG.debug("- Getting the maximum vertex id");
		long maxVertexId = Long.MIN_VALUE;
		try (BufferedReader vertexData = new BufferedReader(new FileReader(vertexFilePath))) {
			for (String vertexLine = vertexData.readLine(); vertexLine != null; vertexLine = vertexData.readLine()) {
				if (vertexLine.isEmpty()) {
					continue;
				}
				maxVertexId = Math.max(maxVertexId, Long.parseLong(vertexLine));
			}
		}
		if(maxVertexId == Long.MIN_VALUE) {
			throw new IllegalArgumentException("No vertex value has been read.");
		}
		return maxVertexId;
	}

	private void parseEdgeList2EdgeValueList() throws IOException {
		LOG.debug("- Adding edge value 1 to each edge in the intermediate file");
		new File(intermediatePath).createNewFile();
		try (BufferedReader edgeData = new BufferedReader(new FileReader(edgeFilePath));
			 PrintWriter edgeValueWriter = new PrintWriter(new File(intermediatePath))) {
			for (String edgeLine = edgeData.readLine(); edgeLine != null; edgeLine = edgeData.readLine()) {
				if (edgeLine.isEmpty()) {
					continue;
				}
				edgeValueWriter.println(edgeLine + " 1");
			}
		}
	}

	private void parseEdgeValueList2Matrix(long maxVertexId) throws IOException {
		LOG.debug("- Writing .mtx graph format to the intermediate file");
		DefaultExecutor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
			@Override
			protected void processLine(String line, int logLevel) {
				LOG.debug(MarkerManager.getMarker("GRAPHMAT-OUTPUT"), "[GRAPHMAT-OUT] {}", line);
			}
		}, new LogOutputStream() {
			@Override
			protected void processLine(String line, int logLevel) {
				LOG.debug(MarkerManager.getMarker("GRAPHMAT-OUTPUT"), "[GRAPHMAT-ERR] {}", line);
			}
		}));
		executor.setExitValues(null);

		CommandLine commandLine = new CommandLine(Paths.get(graphmatParserPath).toFile());
		commandLine.addArgument("--selfloops", false);
		commandLine.addArgument("1", false);
		commandLine.addArgument("--duplicatededges", false);
		commandLine.addArgument("1", false);
		commandLine.addArgument("--inputformat", false);
		commandLine.addArgument("1", false);
		commandLine.addArgument("--inputheader", false);
		commandLine.addArgument("0", false);
		commandLine.addArgument("--outputheader", false);
		commandLine.addArgument("1", false);
		commandLine.addArgument("--nvertices", false);
		commandLine.addArgument(String.valueOf(maxVertexId), false);
		commandLine.addArgument(intermediatePath, false);
		commandLine.addArgument(outputPath, false);

		System.out.println(commandLine.toString());
		executor.execute(commandLine);
	}

	/**
	 * Parses a graph in Graphalytics' VE format, writes the graph to a new file in adjacency list format, and returns
	 * a mapping of vertex ids from the original graph to vertex ids in the output graph.
	 *
	 * @param vertexFilePath   the path of the vertex list for the input graph
	 * @param edgeFilePath     the path of the edge list for the input graph
	 * @param outputPath       the path to write the graph in adjacency list format to
	 * @param isGraphDirected  true iff the graph is directed
	 * @param numberOfVertices the number of vertices in the graph
	 * @return a mapping of vertex ids from the original graph to vertex ids in the output graph
	 * @throws IOException iff an exception occurred while parsing the input graph or writing the output graph
	 */
	public static void parseGraphAndWriteAdjacencyList(String vertexFilePath, String edgeFilePath, String intermediatePath,
													   String outputPath, boolean isGraphDirected, int numberOfVertices, String graphmatParserPath) throws IOException {
		new GraphParser(vertexFilePath, edgeFilePath, intermediatePath, outputPath, isGraphDirected, numberOfVertices, graphmatParserPath).parseAndWrite();
	}

}