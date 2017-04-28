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

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import nl.tudelft.granula.archiver.PlatformArchive;
import nl.tudelft.granula.modeller.job.JobModel;
import nl.tudelft.granula.modeller.platform.Graphmat;
import nl.tudelft.granula.util.FileUtil;
import org.apache.commons.io.output.TeeOutputStream;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.report.result.BenchmarkResult;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.granula.GranulaAwarePlatform;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.domain.graph.PropertyList;
import science.atlarge.graphalytics.domain.graph.PropertyType;
import science.atlarge.graphalytics.graphmat.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.graphmat.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.graphmat.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.graphmat.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.graphmat.algorithms.sssp.SingleSourceShortestPathJob;
import science.atlarge.graphalytics.graphmat.algorithms.wcc.WeaklyConnectedComponentsJob;
import org.json.simple.JSONObject;

/**
 * GraphMat platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public class GraphmatPlatform implements GranulaAwarePlatform {


	public static final String PLATFORM_NAME = "graphmat";
	public static final String PLATFORM_PROPERTIES_FILE = PLATFORM_NAME + ".properties";
	private static final String BENCHMARK_PROPERTIES_FILE = "benchmark.properties";
	private static final String GRANULA_PROPERTIES_FILE = "granula.properties";

	public static final String GRANULA_ENABLE_KEY = "benchmark.run.granula.enabled";
	public static final String RUN_COMMAND_FORMAT_KEY = "graphmat.command.run";
	public static final String CONVERT_COMMAND_FORMAT_KEY = "graphmat.command.convert";
	public static final String INTERMEDIATE_DIR_KEY = "graphmat.intermediate-dir";

	public static String BINARY_DIRECTORY = "./bin/standard";
	public static final String FORMAT_CONVERT_BINARY_NAME = BINARY_DIRECTORY + "/format_convert";
	public static final String MTX_CONVERT_BINARY_NAME = BINARY_DIRECTORY + "/graph_convert";

	private Configuration platformConfig;
	private String intermediateGraphFile;
	private String graphFile;
	private Long2LongMap vertexTranslation;


	private static final Logger LOG = LogManager.getLogger();

	private static PrintStream sysOut;
	private static PrintStream sysErr;


	public GraphmatPlatform() {
		LOG.info("Parsing GraphMat configuration file.");

		Configuration benchmarkConfig;
		Configuration granulaConfig;
		try {
			platformConfig = new PropertiesConfiguration(PLATFORM_PROPERTIES_FILE);
			benchmarkConfig = new PropertiesConfiguration(BENCHMARK_PROPERTIES_FILE);
			granulaConfig = new PropertiesConfiguration(GRANULA_PROPERTIES_FILE);
		} catch (Exception e) {
			LOG.warn("Could not find or load \"{}\"", PLATFORM_PROPERTIES_FILE);
			LOG.warn("Could not find or load \"{}\"", BENCHMARK_PROPERTIES_FILE);
			LOG.warn("Could not find or load \"{}\"", GRANULA_PROPERTIES_FILE);
			platformConfig = new PropertiesConfiguration();
			benchmarkConfig = new PropertiesConfiguration();
			granulaConfig = new PropertiesConfiguration();
		}
		boolean granulaEnabled = granulaConfig.getBoolean(GRANULA_ENABLE_KEY, false);
		BINARY_DIRECTORY = granulaEnabled ? "./bin/granula": BINARY_DIRECTORY;
	}



	@Override
	public void uploadGraph(FormattedGraph formattedGraph) throws Exception {
		LOG.info("Preprocessing graph \"{}\". Currently disabled (not needed).", formattedGraph.getName());

		if (formattedGraph.getNumberOfVertices() > Integer.MAX_VALUE || formattedGraph.getNumberOfEdges() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("GraphMat does not support more than " + Integer.MAX_VALUE + " vertices/edges");
		}

		String intermediateFile = createIntermediateFile(formattedGraph.getName(), "txt0");
		String outputFile = createIntermediateFile(formattedGraph.getName(), "mtx");

		// Convert from Graphalytics VE format to intermediate format
		vertexTranslation = GraphConverter.parseAndWrite(formattedGraph, intermediateFile);
                String vertexTranslationFile = createIntermediateFile(formattedGraph.getName() + "_vertex_translation", "bin");
                BinIO.storeObject(vertexTranslation, vertexTranslationFile);
                LOG.info("Stored vertex translation in: {}", vertexTranslationFile);

		// Check if graph has weights
		boolean isWeighted = false;
		int weightType = 0;

		if (formattedGraph.hasEdgeProperties()) {
			PropertyList pl = formattedGraph.getEdgeProperties();

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
		boolean isDirected = formattedGraph.isDirected();
		String cmdFormat = platformConfig.getString(CONVERT_COMMAND_FORMAT_KEY, "%s %s");
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
	public void prepare(BenchmarkRun benchmarkRun) {

	}

	@Override
	public void preprocess(BenchmarkRun benchmarkRun) {
		startPlatformLogging(benchmarkRun.getLogDir().resolve("platform").resolve("driver.logs"));
	}

	@Override
	public boolean execute(BenchmarkRun benchmarkRun) throws PlatformExecutionException {

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		Object params = benchmarkRun.getAlgorithmParameters();
		GraphmatJob job;

		try {
			setupGraph(benchmarkRun.getFormattedGraph());
		} catch (Exception e) {
			e.printStackTrace();
		}
		boolean isDirected = benchmarkRun.getGraph().isDirected();

                boolean translateVertexProperty = false;
		switch (algorithm) {
			case BFS:
				job = new BreadthFirstSearchJob(platformConfig, graphFile, vertexTranslation, (BreadthFirstSearchParameters) params, benchmarkRun.getId());
				break;
			case PR:
				job = new PageRankJob(platformConfig, graphFile, vertexTranslation, (PageRankParameters) params, benchmarkRun.getId());
				break;
			case WCC:
				job = new WeaklyConnectedComponentsJob(platformConfig, graphFile, vertexTranslation, benchmarkRun.getId());
				break;
			case SSSP:
				job = new SingleSourceShortestPathJob(platformConfig, graphFile, vertexTranslation, (SingleSourceShortestPathsParameters) params, benchmarkRun.getId());
				break;
			case CDLP:
                                translateVertexProperty = true;
				job = new CommunityDetectionLPJob(platformConfig, graphFile, isDirected ? "1" : "0", vertexTranslation, (CommunityDetectionLPParameters) params, benchmarkRun.getId());
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(platformConfig, graphFile, isDirected ? "1" : "0", vertexTranslation, benchmarkRun.getId());
				break;
			default:
				throw new PlatformExecutionException("Not yet implemented.");
		}

		String intermediateOutputPath = null;
		boolean outputEnabled = benchmarkRun.isOutputRequired();

		try{
			if (outputEnabled) {
				intermediateOutputPath = createIntermediateFile("output", "txt0");
				job.setOutputPath(intermediateOutputPath);
			}

			job.execute();

			if (outputEnabled) {
				Path outputFile = benchmarkRun.getOutputDir().resolve(benchmarkRun.getName());
				OutputConverter.parseAndWrite(
						intermediateOutputPath,
						outputFile.toAbsolutePath().toString(),
						vertexTranslation,
                                                translateVertexProperty);
			}
		} catch(Exception e) {
			throw new PlatformExecutionException("failed to execute command", e);
		} finally {
			if (intermediateOutputPath != null) {
				tryDeleteIntermediateFile(intermediateOutputPath);
			}
		}

		return true;
	}

	@Override
	public void cleanup(BenchmarkRun benchmarkRun) {

	}

	@Override
	public BenchmarkMetrics postprocess(BenchmarkRun benchmarkRun) {
		stopPlatformLogging();

		String logs = FileUtil.readFile(benchmarkRun.getLogDir().resolve("platform").resolve("driver.logs"));

		Long startTime = null;
		Long endTime = null;

		for (String line : logs.split("\n")) {
			try {
				if (line.contains("Processing starts at: ")) {
					String[] lineParts = line.split("\\s+");
					startTime = Long.parseLong(lineParts[lineParts.length - 1]);
				}

				if (line.contains("Processing ends at: ")) {
					String[] lineParts = line.split("\\s+");
					endTime = Long.parseLong(lineParts[lineParts.length - 1]);
				}
			} catch (Exception e) {
				LOG.error(String.format("Cannot parse line: %s", line));
				e.printStackTrace();
			}

		}

		if(startTime != null && endTime != null) {

			BenchmarkMetrics metrics = new BenchmarkMetrics();
			metrics.setProcessingTime(endTime - startTime);
			return metrics;
		} else {

			return new BenchmarkMetrics();
		}
	}


	@Override
	public void deleteGraph(FormattedGraph formattedGraph) {
		tryDeleteIntermediateFile(intermediateGraphFile);
		// TODO parametrize graph conversion splits
		for (int i = 0; i < 16; i++) {
			tryDeleteIntermediateFile(graphFile + i);
		}
	}

	private String createIntermediateFile(String name, String extension) throws IOException {
		String dir = platformConfig.getString(INTERMEDIATE_DIR_KEY, null);

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

	private void setupGraph(FormattedGraph formattedGraph) throws Exception {

		String intermediateFile = createIntermediateFile(formattedGraph.getName(), "txt");
		String outputFile = createIntermediateFile(formattedGraph.getName(), "mtx");


		String vertexTranslationFile = createIntermediateFile(formattedGraph.getName() + "_vertex_translation", "bin");
		vertexTranslation = (Long2LongMap)BinIO.loadObject(vertexTranslationFile);


		this.intermediateGraphFile = intermediateFile;
		this.graphFile = outputFile;
	}


	public static void runCommand(String format, String binaryName, List<String> args) throws InterruptedException, IOException  {
		String argsString = "";
		for (String arg: args) {
			argsString += arg + " ";
		}

		System.out.println("format = " + format);
		System.out.println("binaryName = " + binaryName);
		System.out.println("argsString = " + argsString);
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
	public String getPlatformName() {
		return PLATFORM_NAME;
	}


	@Override
	public JobModel getJobModel() {
		return new JobModel(new Graphmat());
	}

	@Override
	public void enrichMetrics(BenchmarkResult benchmarkResult, Path arcDirectory) {
		try {
			PlatformArchive platformArchive = PlatformArchive.readArchive(arcDirectory);
			JSONObject processGraph = platformArchive.operation("ProcessGraph");
			Integer procTime = Integer.parseInt(platformArchive.info(processGraph, "Duration"));
			BenchmarkMetrics metrics = benchmarkResult.getMetrics();
			metrics.setProcessingTime(procTime);
		} catch(Exception e) {
			LOG.error("Failed to enrich metrics.");
		}
	}

	public static void startPlatformLogging(Path fileName) {
		sysOut = System.out;
		sysErr = System.err;
		try {
			File file = null;
			file = fileName.toFile();
			file.getParentFile().mkdirs();
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			TeeOutputStream bothStream =new TeeOutputStream(System.out, fos);
			PrintStream ps = new PrintStream(bothStream);
			System.setOut(ps);
			System.setErr(ps);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("cannot redirect to output file");
		}
	}

	public static void stopPlatformLogging() {
		System.setOut(sysOut);
		System.setErr(sysErr);
	}
}
