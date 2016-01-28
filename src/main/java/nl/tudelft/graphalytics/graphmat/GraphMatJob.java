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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.exec.*;
import org.apache.commons.exec.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for all jobs in the GraphMat benchmark suite. Configures and executes a GraphMat job using the parameters
 * and executable specified by the subclass for a specific algorithm.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public abstract class GraphMatJob {
	private static final Logger LOG = LogManager.getLogger(GraphMatJob.class);

	private Configuration config;
	private String graphPath;
	private String outputPath;

	public GraphMatJob(Configuration config, String graphPath) {
		this.config = config;
		this.graphPath = graphPath;
		this.outputPath = null;
	}
	
	public void setOutputPath(String file) {
		this.outputPath = file;
	}
	
	abstract protected String getExecutable();
	abstract protected void addJobArguments(List<String> args);
	
	public void execute() throws IOException, InterruptedException {
		List<String> args = new ArrayList<>();
		args.add(graphPath);
		addJobArguments(args);
		
		if (outputPath != null) {
			args.add(outputPath);
		}
		
		String cmdFormat = config.getString(GraphMatPlatform.RUN_COMMAND_FORMAT_KEY, "%s %s");
		GraphMatPlatform.runCommand(cmdFormat, getExecutable(), args);
	}
}
