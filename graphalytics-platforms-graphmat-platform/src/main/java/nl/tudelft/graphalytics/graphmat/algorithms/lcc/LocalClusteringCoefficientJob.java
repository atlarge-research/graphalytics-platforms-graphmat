package nl.tudelft.graphalytics.graphmat.algorithms.lcc;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.graphmat.GraphMatJob;

public class LocalClusteringCoefficientJob extends GraphMatJob {

	public LocalClusteringCoefficientJob(Configuration config, String graphPath, Long2LongMap vertexTranslation, String jobId) {
		super(config, graphPath, vertexTranslation, jobId);
	}
		
	@Override
	protected String getExecutable() {
		return "lcc";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		// None
		args.add(jobId);
	}
}
