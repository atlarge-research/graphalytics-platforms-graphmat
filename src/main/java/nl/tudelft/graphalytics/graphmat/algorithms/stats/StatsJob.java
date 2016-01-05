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
package nl.tudelft.graphalytics.graphmat.algorithms.stats;

import nl.tudelft.graphalytics.graphmat.GraphMatJob;
import nl.tudelft.graphalytics.graphmat.config.JobConfiguration;
import org.apache.commons.exec.CommandLine;

import java.nio.file.Paths;

/**
 * Connected components job implementation for GraphMat. This class is responsible for formatting CC-specific
 * arguments to be passed to the GraphMat executable, and does not include the implementation of the algorithm.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public final class StatsJob extends GraphMatJob {

	/**
	 * Creates a new ConnectedComponentsJob object with all mandatory parameters specified.
	 *
	 * @param jobConfiguration the generic GraphMat configuration to use for this job
	 * @param graphInputPath   the path of the input graph
	 * @param graphOutputPath  the path of the output graph
	 */
	public StatsJob(JobConfiguration jobConfiguration, String graphInputPath, String graphOutputPath) {
		super(jobConfiguration, graphInputPath, graphOutputPath);
	}

	@Override
	protected void appendAlgorithm(CommandLine commandLine) {
		commandLine.addArgument(Paths.get(jobConfiguration.getExecutableDirectory(), getExecutableName()).toString(), false);
	}

	@Override
	protected void appendAlgorithmParameters(CommandLine commandLine) {
	}

	@Override
	protected String getExecutableName() {
		return "TriangleCounting";
	}


	@Override
	protected boolean usesEdgeProperties() {
		return false;
	}

}
