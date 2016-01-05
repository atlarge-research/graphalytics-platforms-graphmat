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
package nl.tudelft.graphalytics.graphmat.config;

import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import org.apache.commons.configuration.Configuration;

import java.nio.file.Paths;

/**
 * Parser for the Graphalytics GraphMat properties file. This class extracts all properties related to GraphMat jobs
 * and stores them in a JobConfiguration object.
 *
 * @author Wing Lung Ngai
 * @author Tim Hegeman
 */
public final class JobConfigurationParser {

	public static final String GRAPHMAT_HOME_KEY = "graphmat.home";
	public static final String GRAPHMAT_NUM_WORKER_THREADS_KEY = "graphmat.num-worker-threads";

	private final Configuration graphmatConfig;

	private JobConfigurationParser(Configuration graphmatConfig) {
		this.graphmatConfig = graphmatConfig;
	}

	private JobConfiguration parse() throws InvalidConfigurationException {
		// Load mandatory configuration properties
		String graphmatHome = ConfigurationUtil.getString(graphmatConfig, GRAPHMAT_HOME_KEY);
		String graphmatBinariesPath = Paths.get(graphmatHome, "bin").toString();
		JobConfiguration jobConfiguration = new JobConfiguration(graphmatBinariesPath);

		// Load optional configuration properties
		parseNumWorkerThreads(jobConfiguration);

		return jobConfiguration;
	}


	private void parseNumWorkerThreads(JobConfiguration jobConfiguration) {
		Integer numWorkers = graphmatConfig.getInteger(GRAPHMAT_NUM_WORKER_THREADS_KEY, null);
		if (numWorkers != null) {
			jobConfiguration.setNumberOfWorkerThreads(numWorkers);
		}
	}

	public static JobConfiguration parseGraphMatPropertiesFile(Configuration graphmatConfig)
			throws InvalidConfigurationException {
		return new JobConfigurationParser(graphmatConfig).parse();
	}

}
