package nl.tudelft.graphalytics.graphmat.algorithms.lcc;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.graphmat.GraphMatJob;

public class LocalClusteringCoefficientJob extends GraphMatJob {

        private String isDirected;

	public LocalClusteringCoefficientJob(Configuration config, String graphPath, String isDirected, Long2LongMap vertexTranslation) {
		super(config, graphPath, vertexTranslation);
                this.isDirected = isDirected;
	}
		
	@Override
	protected String getExecutable() {
		return "lcc";
	}

	@Override
	protected void addJobArguments(List<String> args) {
		args.add(isDirected);
	}
}
