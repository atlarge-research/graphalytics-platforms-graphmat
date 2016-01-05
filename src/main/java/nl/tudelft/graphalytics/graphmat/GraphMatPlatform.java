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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.granula.GranulaAwarePlatform;
import nl.tudelft.graphalytics.granula.GranulaManager;
import nl.tudelft.graphalytics.graphmat.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.graphmat.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.graphmat.algorithms.stats.StatsJob;
import nl.tudelft.graphalytics.graphmat.config.JobConfiguration;
import nl.tudelft.graphalytics.graphmat.config.JobConfigurationParser;
import nl.tudelft.graphalytics.graphmat.reporting.logging.GraphMatLogger;
import nl.tudelft.pds.granula.modeller.graphmat.job.GraphMat;
import nl.tudelft.pds.granula.modeller.model.job.JobModel;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * GraphMat platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public final class GraphMatPlatform implements Platform, GranulaAwarePlatform {

	private static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "graphmat";
	public static final String PROPERTIES_FILENAME = PLATFORM_NAME + ".properties";

	public static final String GRAPHMAT_INTERMEDIATE_DIR_KEY = "graphmat.intermediate-dir";
	public static final String GRAPHMAT_OUTPUT_DIR_KEY = "graphmat.output-dir";

	private Configuration graphmatConfig;
	private JobConfiguration jobConfiguration;
	private String intermediateGraphDirectory;
	private String graphOutputDirectory;
	private String graphmatParserPath;

	private String currentGraphPath;
	private Long2LongMap currentGraphVertexIdTranslation;

	public GraphMatPlatform() {
		try {
			loadConfiguration();
		} catch (InvalidConfigurationException e) {
			// TODO: Implement cleaner exit procedure
			LOG.fatal(e);
			System.exit(-1);
		}
	}

	private void loadConfiguration() throws InvalidConfigurationException {
		LOG.info("Parsing GraphMat configuration file.");

		// Load GraphMat-specific configuration
		try {
			graphmatConfig = new PropertiesConfiguration(PROPERTIES_FILENAME);
		} catch (ConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.warn("Could not find or load \"{}\"", PROPERTIES_FILENAME);
			graphmatConfig = new PropertiesConfiguration();
		}

		// Parse generic job configuration from the GraphMat properties file
		jobConfiguration = JobConfigurationParser.parseGraphMatPropertiesFile(graphmatConfig);

		intermediateGraphDirectory = ConfigurationUtil.getString(graphmatConfig, GRAPHMAT_INTERMEDIATE_DIR_KEY);
		graphOutputDirectory = ConfigurationUtil.getString(graphmatConfig, GRAPHMAT_OUTPUT_DIR_KEY);
		graphmatParserPath = Paths.get(jobConfiguration.getExecutableDirectory(), "graph_converter").toString();

		ensureDirectoryExists(intermediateGraphDirectory, GRAPHMAT_INTERMEDIATE_DIR_KEY);
		ensureDirectoryExists(graphOutputDirectory, GRAPHMAT_OUTPUT_DIR_KEY);
	}

	private static void ensureDirectoryExists(String directory, String property) throws InvalidConfigurationException {
		System.out.println(directory);
		File directoryFile = new File(directory);
		if (directoryFile.exists()) {
			if (!directoryFile.isDirectory()) {
				throw new InvalidConfigurationException("Path \"" + directory + "\" set as property \"" + property +
						"\" already exists, but is not a directory");
			}
			return;
		}

		if (!directoryFile.mkdirs()) {
			throw new InvalidConfigurationException("Unable to create directory \"" + directory +
					"\" set as property \"" + property + "\"");
		}
		LOG.info("Created directory \"{}\" and any missing parent directories", directory);
	}

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Preprocessing graph \"{}\". Currently disabled (not needed).", graph.getName());

		//TODO check if this is true.
		if (graph.getNumberOfVertices() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Graphalytics for GraphMat does not currently support graphs with more than Integer.MAX_VALUE vertices");
		}

		String graphIntermediatePath = Paths.get(intermediateGraphDirectory, graph.getName()+".ev").toString();
		String graphOutputPath = Paths.get(intermediateGraphDirectory, graph.getName()+".mtx").toString();
		GraphParser.parseGraphAndWriteAdjacencyList(
				graph.getVertexFilePath(), graph.getEdgeFilePath(),
				graphIntermediatePath, graphOutputPath,
				graph.getGraphFormat().isDirected(), (int)graph.getNumberOfVertices(), graphmatParserPath);
		currentGraphPath = graphOutputPath;
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {

		Algorithm algorithm = benchmark.getAlgorithm();
		Graph graph = benchmark.getGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		GraphMatJob job;
		String outputGraphPath = Paths.get(graphOutputDirectory, graph.getName() + "-" + algorithm).toString();
		switch (algorithm) {
			case BFS:
				long sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
				job = new BreadthFirstSearchJob(sourceVertex, jobConfiguration, currentGraphPath, outputGraphPath);
				break;
			case PAGERANK:
				job = new PageRankJob(jobConfiguration, currentGraphPath, outputGraphPath);
				break;
			case STATS:
				job = new StatsJob(jobConfiguration, currentGraphPath, outputGraphPath);
				break;
			default:
				// TODO: Implement other algorithms
				throw new PlatformExecutionException("Not yet implemented.");
		}

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), graph.getName());

		int exitCode;
		try {
			exitCode = job.execute();
		} catch (IOException e) {
			throw new PlatformExecutionException("Failed to launch GraphMat", e);
		}

		if (exitCode != 0) {
			throw new PlatformExecutionException("GraphMat completed with a non-zero exit code: " + exitCode);
		}
		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	@Override
	public void deleteGraph(String graphName) {
		// TODO: Implement
		LOG.info("Deleting working copy of graph \"{}\". Not doing anything", graphName);
	}

	@Override
	public String getName() {
		return PLATFORM_NAME;
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.fromExternalConfiguration(graphmatConfig, PROPERTIES_FILENAME);
	}

	@Override
	public void setBenchmarkLogDirectory(Path logDirectory) {
			GraphMatLogger.startPlatformLogging(logDirectory.resolve("OperationLog").resolve("driver.logs"));
	}

	@Override
	public void finalizeBenchmarkLogs(Path logDirectory) {
//			GraphMatLogger.collectYarnLogs(logDirectory);
			// TODO replace with collecting logs from graphmat
//			GraphMatLogger.collectUtilLog(null, null, 0, 0, logDirectory);
			GraphMatLogger.stopPlatformLogging();

	}

	@Override
	public JobModel getGranulaModel() {
		return new GraphMat();
	}
}
