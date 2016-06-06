package nl.tudelft.graphalytics.graphmat.algorithms.cdlp;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.graphmat.GraphMatJob;

public class CommunityDetectionLPJob extends GraphMatJob {

	private final CommunityDetectionLPParameters params;

	public CommunityDetectionLPJob(Configuration config, String graphPath, Long2LongMap vertexTranslation, CommunityDetectionLPParameters params, String jobId) {
		super(config, graphPath, vertexTranslation, jobId);
		this.params = params;
	}
	
	@Override
	protected String getExecutable() {
		return "cd";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		args.add(Integer.toString(params.getMaxIterations()));
		args.add(jobId);
	}
}
