package nl.tudelft.graphalytics.graphmat.algorithms.lcc;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import nl.tudelft.graphalytics.graphmat.GraphMatJob;

public class LocalClusteringCoefficientJob extends GraphMatJob {

	public LocalClusteringCoefficientJob(Configuration config, String graphPath) {
		super(config, graphPath);
	}
		
	@Override
	protected String getExecutable() {
		return "lcc";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		// None
	}
}
