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

/**
 * Collection of GraphMat configuration options.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public final class JobConfiguration {

	private final String executableDirectory;
	private int numberOfWorkerThreads = -1;

	/**
	 * Creates a new JobConfiguration object to capture all GraphMat parameters that are not specific to any algorithm.
	 *
	 * @param executableDirectory the directory containing GraphMat executables
	 */
	public JobConfiguration(String executableDirectory) {
		this.executableDirectory = executableDirectory;
	}

	/**
	 * @return the directory containing GraphMat executables
	 */
	public String getExecutableDirectory() {
		return executableDirectory;
	}

	/**
	 * @return the number of worker threads to use on each node
	 */
	public int getNumberOfWorkerThreads() {
		return numberOfWorkerThreads;
	}

	/**
	 * @param numberOfWorkerThreads the number of worker threads to use on each node
	 */
	public void setNumberOfWorkerThreads(int numberOfWorkerThreads) {
		this.numberOfWorkerThreads = numberOfWorkerThreads;
	}

}
