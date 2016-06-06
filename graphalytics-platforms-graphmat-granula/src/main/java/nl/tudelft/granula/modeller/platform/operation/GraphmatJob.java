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

package nl.tudelft.granula.modeller.platform.operation;

import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.rule.derivation.SimpleSummaryDerivation;
import nl.tudelft.granula.modeller.rule.derivation.time.*;
import nl.tudelft.granula.modeller.rule.linking.EmptyLinking;
import nl.tudelft.granula.modeller.rule.visual.TableVisualization;

import java.util.ArrayList;

public class GraphmatJob extends nl.tudelft.granula.modeller.platform.operation.AbstractOperationModel {

    public GraphmatJob() {
        super(Type.GraphMat, Type.Job);
    }

    public void loadRules() {
        super.loadRules();
        addLinkingRule(new EmptyLinking());
        addInfoDerivation(new JobStartTimeDerivation(1));
        addInfoDerivation(new JobEndTimeDerivation(1));
        addInfoDerivation(new DurationDerivation(2));

        String summary = "A Graphmat.D job executes three child operations: LoadGraph, ProcessGraph, and OffloadGraph. ";
        addInfoDerivation(new SimpleSummaryDerivation(11, summary));

        addVisualDerivation(new TableVisualization(1, "MainInfo",
                new ArrayList<String>() {{
                }}));
    }

}
