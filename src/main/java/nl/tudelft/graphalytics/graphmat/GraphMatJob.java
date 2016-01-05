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

import nl.tudelft.graphalytics.graphmat.config.JobConfiguration;
import org.apache.commons.exec.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all jobs in the GraphMat benchmark suite. Configures and executes a GraphMat job using the parameters
 * and executable specified by the subclass for a specific algorithm.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public abstract class GraphMatJob {

	private static final Logger LOG = LogManager.getLogger();
	private static final Marker GRAPHMAT_OUTPUT_MARKER = MarkerManager.getMarker("GRAPHMAT-OUTPUT");

	protected final JobConfiguration jobConfiguration;
	private final String graphInputPath;
	private final String graphOutputPath;
	private final Map<String, String> envVars;

	/**
	 * Initializes the generic parameters required for running any GraphMat job.
	 *
	 * @param jobConfiguration the generic GraphMat configuration to use for this job
	 * @param graphInputPath   the path of the input graph
	 * @param graphOutputPath  the path of the output graph
	 */
	public GraphMatJob(JobConfiguration jobConfiguration, String graphInputPath, String graphOutputPath) {
		this.jobConfiguration = jobConfiguration;
		this.graphInputPath = graphInputPath;
		this.graphOutputPath = graphOutputPath;
		this.envVars = new HashMap<>();
	}

	/**
	 * Executes the algorithm defined by this job, with the parameters defined by the user.
	 *
	 * @return the exit code of GraphMat
	 * @throws IOException if GraphMat failed to run
	 */
	public int execute() throws IOException {
		envVars.put("KMP_AFFINITY", "scatter");
		configureThreadingParameters();

		CommandLine commandLine = createCommandLineForExecutable();
		commandLine.addArgument("-i", false);
		commandLine.addArgument("all", false);
		appendAlgorithm(commandLine);
		appendGraphPathParameters(commandLine);;
		appendAlgorithmParameters(commandLine);


		LOG.debug("Starting job with command line: {}", commandLine.toString());

		Executor executor = createCommandExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out));
		return executor.execute(commandLine, envVars);
	}

	protected CommandLine createCommandLineForExecutable() {
		return new CommandLine("numactl");
	}

	private Executor createCommandExecutor() {
		DefaultExecutor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(
				(LOG.isDebugEnabled() ? new JobOutLogger() : null),
				(LOG.isInfoEnabled() ? new JobErrLogger() : null)
		));
		executor.setExitValues(null);
		return executor;
	}

	private void appendGraphPathParameters(CommandLine commandLine) {
		commandLine.addArgument(Paths.get(graphInputPath).toString(), false);
	}

//	private void appendGraphTypeParameters(CommandLine commandLine) {
//		commandLine.addArgument("-load_eprops=" + (usesEdgeProperties() ? "1" : "0"));
//		commandLine.addArgument("-graph_export_type=" + getGraphExportType());
//	}

	private void configureThreadingParameters() {
		envVars.put("OMP_NUM_THREADS", "1");

		if (jobConfiguration.getNumberOfWorkerThreads() > 0) {
			envVars.put("OMP_NUM_THREADS", String.valueOf(jobConfiguration.getNumberOfWorkerThreads()));
		}
	}

	/**
	 * Appends the algorithm for the GraphMat executable to a CommandLine object.
	 *
	 * @param commandLine the CommandLine to append arguments to
	 */
	protected abstract void appendAlgorithm(CommandLine commandLine);

	/**
	 * Appends the algorithm-specific parameters for the GraphMat executable to a CommandLine object.
	 *
	 * @param commandLine the CommandLine to append arguments to
	 */
	protected abstract void appendAlgorithmParameters(CommandLine commandLine);

	/**
	 * @return the name of the algorithm-specific GraphMat executable
	 */
	protected abstract String getExecutableName();


	public Map<String, String> getEnvVars() {
		return envVars;
	}

	/**
	 * @return true iff the algorithm requires edge properties to be read from the input graph
	 */
	protected abstract boolean usesEdgeProperties();

	/**
	 * Helper class for logging standard output from GraphMat to log4j.
	 */
	private static final class JobOutLogger extends LogOutputStream {

		@Override
		protected void processLine(String line, int logLevel) {
			LOG.debug(GRAPHMAT_OUTPUT_MARKER, "[GRAPHMAT-OUT] {}", line);
		}

	}

	/**
	 * Helper class for logging standard error from GraphMat to log4j.
	 */
	private static final class JobErrLogger extends LogOutputStream {

		@Override
		protected void processLine(String line, int logLevel) {
			LOG.info(GRAPHMAT_OUTPUT_MARKER, "[GRAPHMAT-ERR] {}", line);
		}

	}

}
