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
package science.atlarge.graphalytics.graphmat.algorithms.cdlp;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.graphmat.GraphmatJob;

public class CommunityDetectionLPJob extends GraphmatJob {

	private final CommunityDetectionLPParameters params;
        private String isDirected;

	public CommunityDetectionLPJob(Configuration config, String graphPath, String isDirected, Long2LongMap vertexTranslation, CommunityDetectionLPParameters params, String jobId) {
		super(config, graphPath, vertexTranslation, jobId);
		this.params = params;
                this.isDirected = isDirected;
	}
	
	@Override
	protected String getExecutable() {
		return "cd";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		args.add(Integer.toString(params.getMaxIterations()));
		args.add(jobId);
		args.add(isDirected);
	}
}
