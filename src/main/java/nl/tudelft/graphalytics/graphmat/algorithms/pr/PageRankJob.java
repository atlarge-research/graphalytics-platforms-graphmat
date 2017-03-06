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
package nl.tudelft.graphalytics.graphmat.algorithms.pr;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.graphmat.GraphMatJob;	

/**
 * PR
 *
 * @author Wing Lung Ngai
 * @author Tim Hegeman
 */
public final class PageRankJob extends GraphMatJob {

	private final PageRankParameters params;
	
	/**
	 * Creates a new PageRankJob object with all mandatory parameters specified.
	 *
	 * @param jobConfiguration the generic GraphMat configuration to use for this job
	 * @param graphInputPath   the path of the input graph
	 * @param graphOutputPath  the path of the output graph
	 */
	public PageRankJob(Configuration config, String graphPath, Long2LongMap vertexTranslation, PageRankParameters params, String jobId) {
		super(config, graphPath, vertexTranslation, jobId);
		this.params = params;
	}

	@Override
	protected String getExecutable() {
		return "pr";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		args.add(Integer.toString(params.getNumberOfIterations()));
		args.add(Double.toString(params.getDampingFactor()));
		args.add(jobId);
	}

}
