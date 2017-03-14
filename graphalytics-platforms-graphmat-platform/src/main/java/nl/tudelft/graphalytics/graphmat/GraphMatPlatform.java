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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import nl.tudelft.graphalytics.BenchmarkMetrics;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.NestedConfiguration;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.domain.graph.PropertyList;
import nl.tudelft.graphalytics.domain.graph.PropertyType;
import nl.tudelft.graphalytics.graphmat.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.graphmat.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.graphmat.algorithms.lcc.LocalClusteringCoefficientJob;
import nl.tudelft.graphalytics.graphmat.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.graphmat.algorithms.sssp.SingleSourceShortestPathJob;
import nl.tudelft.graphalytics.graphmat.algorithms.wcc.WeaklyConnectedComponentsJob;

/**
 * GraphMat platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public class GraphMatPlatform implements Platform {

	private static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "graphmat";
	public static final String PROPERTIES_FILENAME = PLATFORM_NAME + ".properties";

	public static final String RUN_COMMAND_FORMAT_KEY = "graphmat.command.run";
	public static final String CONVERT_COMMAND_FORMAT_KEY = "graphmat.command.convert";
	public static final String INTERMEDIATE_DIR_KEY = "graphmat.intermediate-dir";

	public static String BINARY_DIRECTORY = "./bin/standard";
	public static final String FORMAT_CONVERT_BINARY_NAME = BINARY_DIRECTORY + "/format_convert";
	public static final String MTX_CONVERT_BINARY_NAME = BINARY_DIRECTORY + "/graph_convert";

	private Configuration config;
	private String intermediateGraphFile;
	private String graphFile;
	private Long2LongMap vertexTranslation;

	public GraphMatPlatform() {
		LOG.info("Parsing GraphMat configuration file.");

		try {
			config = new PropertiesConfiguration(PROPERTIES_FILENAME);
		} catch (Exception e) {
			LOG.warn("Could not find or load \"{}\"", PROPERTIES_FILENAME);
			config = new PropertiesConfiguration();
		}
	}

	private String createIntermediateFile(String name, String extension) throws IOException {
		String dir = config.getString(INTERMEDIATE_DIR_KEY, null);

		if (dir != null) {
			File f = new File(dir);

			if (!f.isDirectory() && !f.mkdirs()) {
				throw new IOException("failed to create intermediate directory: " + dir);
			}

			return f + "/" + name + "." + extension;
		} else {
			return File.createTempFile(name, "." + extension).getAbsolutePath();
		}
	}

	private void tryDeleteIntermediateFile(String path) {
		if (!new File(path).delete()) {
			LOG.warn("failed to delete intermediate file '{}'", path);
		}
	}

	private void setupGraph(Graph graph) throws Exception {

		String intermediateFile = createIntermediateFile(graph.getName(), "txt0");
		String outputFile = createIntermediateFile(graph.getName(), "mtx");


		vertexTranslation = GraphConverter.parseAndWrite(graph, intermediateFile);


		this.intermediateGraphFile = intermediateFile;
		this.graphFile = outputFile;
	}


	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Preprocessing graph \"{}\". Currently disabled (not needed).", graph.getName());

		if (graph.getNumberOfVertices() > Integer.MAX_VALUE || graph.getNumberOfEdges() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("GraphMat does not support more than " + Integer.MAX_VALUE + " vertices/edges");
		}

		String intermediateFile = createIntermediateFile(graph.getName(), "txt0");
		String outputFile = createIntermediateFile(graph.getName(), "mtx");

		// Convert from Graphalytics VE format to intermediate format
		vertexTranslation = GraphConverter.parseAndWrite(graph, intermediateFile);

		// Check if graph has weights
		boolean isWeighted = false;
		int weightType = 0;

		if (graph.hasEdgeProperties()) {
			PropertyList pl = graph.getEdgeProperties();

			if (pl.size() != 1) {
				throw new IllegalArgumentException(
						"GraphMat does not support more than one property");
			}

			PropertyType t = pl.get(0).getType();

			if (t.equals(PropertyType.INTEGER)) {
				weightType = 0;
			}  else if (t.equals(PropertyType.REAL)) {
				weightType = 1;
			} else {
				throw new IllegalArgumentException(
						"GraphMat does not support properties of type: " + t);
			}

			isWeighted = true;
		}


		// Convert from intermediate format to MTX format
		boolean isDirected = graph.isDirected();
		String cmdFormat = config.getString(CONVERT_COMMAND_FORMAT_KEY, "%s %s");
		List<String> args = new ArrayList<>();

		args.clear();
		args.add("--selfloops=0");
		args.add("--duplicatededges=0");
		if (!isDirected) args.add("--bidirectional");
		args.add("--inputformat=1");
		args.add("--outputformat=0");
		args.add("--inputheader=0");
		args.add("--outputheader=1");
		args.add("--inputedgeweights=" + (isWeighted ? "1" : "0"));
		args.add("--outputedgeweights=" + (isWeighted ? "1" : "2"));
		args.add("--edgeweighttype=" + weightType);
		//args.add("--split=16");
		//args.add(intermediateFile);
		args.add(intermediateFile.substring(0, intermediateFile.length()-1));
		args.add(outputFile);
		runCommand(cmdFormat, MTX_CONVERT_BINARY_NAME, args);

		// Success! Set paths to intermediate and output files
		this.intermediateGraphFile = intermediateFile;
		this.graphFile = outputFile;
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {

		Algorithm algorithm = benchmark.getAlgorithm();
		Object params = benchmark.getAlgorithmParameters();
		GraphMatJob job;

		try {
			setupGraph(benchmark.getGraph());
		} catch (Exception e) {
			e.printStackTrace();
		}
		boolean isDirected = benchmark.getGraph().isDirected();

		switch (algorithm) {
			case BFS:
				job = new BreadthFirstSearchJob(config, graphFile, vertexTranslation, (BreadthFirstSearchParameters) params, benchmark.getId());
				break;
			case PR:
				job = new PageRankJob(config, graphFile, vertexTranslation, (PageRankParameters) params, benchmark.getId());
				break;
			case WCC:
				job = new WeaklyConnectedComponentsJob(config, graphFile, vertexTranslation, benchmark.getId());
				break;
			case SSSP:
				job = new SingleSourceShortestPathJob(config, graphFile, vertexTranslation, (SingleSourceShortestPathsParameters) params, benchmark.getId());
				break;
			case CDLP:
				job = new CommunityDetectionLPJob(config, graphFile, vertexTranslation, (CommunityDetectionLPParameters) params, benchmark.getId());
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(config, graphFile, isDirected ? "1" : "0", vertexTranslation, benchmark.getId());
				break;
			default:
				throw new PlatformExecutionException("Not yet implemented.");
		}

		String intermediateOutputPath = null;
		boolean outputEnabled = benchmark.isOutputRequired();

		try{
			if (outputEnabled) {
				intermediateOutputPath = createIntermediateFile("output", "txt0");
				job.setOutputPath(intermediateOutputPath);
			}

			job.execute();

			if (outputEnabled) {
				OutputConverter.parseAndWrite(
						intermediateOutputPath,
						benchmark.getOutputPath(),
						vertexTranslation);
			}
		} catch(Exception e) {
			throw new PlatformExecutionException("failed to execute command", e);
		} finally {
			if (intermediateOutputPath != null) {
				tryDeleteIntermediateFile(intermediateOutputPath);
			}
		}

		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	@Override
	public void deleteGraph(String graphName) {
		tryDeleteIntermediateFile(intermediateGraphFile);
		// TODO parametrize graph conversion splits
		for (int i = 0; i < 16; i++) {
			tryDeleteIntermediateFile(graphFile + i);
		}
	}

	@Override
	public BenchmarkMetrics retrieveMetrics() {
		return new BenchmarkMetrics(); //TODO implements this method.
	}

	public static void runCommand(String format, String binaryName, List<String> args) throws InterruptedException, IOException  {
		String argsString = "";
		for (String arg: args) {
			argsString += arg + " ";
		}

		String cmd = String.format(format, binaryName, argsString);

		LOG.info("running command: {}", cmd);

		ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));
//		pb.redirectErrorStream(true);
//		pb.redirectError(Redirect.INHERIT);
//		pb.redirectOutput(Redirect.INHERIT);
//		pb.inheritIO();
		pb.redirectErrorStream(true);
		Process process = pb.start();

		InputStreamReader isr = new InputStreamReader(process.getInputStream());
		BufferedReader br = new BufferedReader(isr);
		String line;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}

		int exit = process.waitFor();

		if (exit != 0) {
			throw new IOException("unexpected error code");
		}
	}

	@Override
	public String getName() {
		return PLATFORM_NAME;
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.fromExternalConfiguration(config, PROPERTIES_FILENAME);
	}
}
