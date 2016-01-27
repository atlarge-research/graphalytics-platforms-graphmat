package nl.tudelft.graphalytics.graphmat.algorithms.cdlp;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.graphmat.GraphMatJob;

public class CommunityDetectionLPJob extends GraphMatJob {

	private final CommunityDetectionLPParameters params;

	public CommunityDetectionLPJob(Configuration config, String graphPath, CommunityDetectionLPParameters params) {
		super(config, graphPath);
		this.params = params;
	}
	
	@Override
	protected String getExecutable() {
		return "cd";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		args.add(Integer.toString(params.getMaxIterations()));
	}
}
