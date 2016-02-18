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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class OutputConverter {

	public static void parseAndWrite(String inputFile, String outputFile, Long2LongMap vertexTranslation) throws IOException {
		try (BufferedReader r = new BufferedReader(new FileReader(inputFile));
				PrintWriter p = new PrintWriter(new FileWriter(outputFile))) {

			// Flip vertex translation, since we need to translate from new vertex ids to old vertex ids
			Long2LongMap revVertexTranslation = new Long2LongOpenHashMap();
			for (Long2LongMap.Entry e: vertexTranslation.long2LongEntrySet()) {
				revVertexTranslation.put(e.getLongValue(), e.getLongKey());
			}

			String line;

			while ((line = r.readLine()) != null) {
				String[] parts = line.split(" *", 2);

				if (parts.length == 2) {
					long vertexId = Long.parseLong(parts[0]);

					p.print(vertexTranslation.get(vertexId));
					p.print(" ");
					p.print(parts[1]);
					p.print("\n");
				} else {
					p.print(line);
					p.print("\n");
				}
			}

		}
	}
}
