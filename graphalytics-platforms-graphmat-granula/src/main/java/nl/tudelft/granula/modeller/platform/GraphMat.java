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

package nl.tudelft.granula.modeller.platform;

import nl.tudelft.granula.modeller.job.Job;
import nl.tudelft.granula.modeller.job.Overview;
import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.platform.operation.*;
import nl.tudelft.granula.modeller.rule.derivation.DerivationRule;
import nl.tudelft.granula.modeller.rule.extraction.GraphmatExtractionRule;
import nl.tudelft.granula.modeller.platform.info.BasicInfo;
import nl.tudelft.granula.modeller.platform.info.Source;
import nl.tudelft.granula.modeller.rule.filling.UniqueOperationFilling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphMat extends PlatformModel {

    public GraphMat() {
        super();
        addOperationModel(new GraphmatJob());
        addOperationModel(new LoadGraph());
        addOperationModel(new OffloadGraph());
        addOperationModel(new ProcessGraph());
        addOperationModel(new BspSuperstep());
    }

    public void loadRules() {

        addFillingRule(new UniqueOperationFilling(2, Type.GraphMat, Type.Job));

        addInfoDerivation(new JobNameDerivationRule(4));
        addInfoDerivation(new JobInfoRule(20));
        addExtraction(new GraphmatExtractionRule(1));

    }


    protected class JobInfoRule extends DerivationRule {

        public JobInfoRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
            platform.setName("A GraphMat job");
            platform.setType("GraphMat");

            Job job = platform.getJob();
            Overview overview = job.getOverview();

            overview.setDescription("GraphMat.D, developed by Intel, " +
                    "maps Pregel-like vertex programs to high-performance sparse matrix operations, " +
                    "a well-developed area of HPC. GraphMat supports two different backends which need to be selected manually: " +
                    "a single-machine shared-memory backend and a distributed MPI-based backend.");

            Operation jobOper = platform.findOperation(Type.GraphMat, Type.Job);
            jobOper.parentId = null;
            platform.addRoot(jobOper.getUuid());

            try {
                nl.tudelft.granula.modeller.platform.operation.Operation processGraph = platform.findOperation(Type.GraphMat, Type.ProcessGraph);
                long processingTime = Long.parseLong(processGraph.getInfo("Duration").getValue());

                nl.tudelft.granula.modeller.platform.operation.Operation loadGraph = platform.findOperation(Type.GraphMat, Type.LoadGraph);
                long loadTime = Long.parseLong(loadGraph.getInfo("Duration").getValue());

                nl.tudelft.granula.modeller.platform.operation.Operation topOperation = platform.findOperation(Type.GraphMat, Type.Job);
                long totalTime = Long.parseLong(topOperation.getInfo("Duration").getValue());

                long otherTime = totalTime - loadTime - processingTime;

                Map<String, Long> breakDown = new HashMap<>();
                breakDown.put("ProcessingTime", processingTime);
                breakDown.put("IOTime", loadTime);
                breakDown.put("Overhead", otherTime);
                overview.setBreakDown(breakDown);
            } catch (Exception e) {
                System.out.println(String.format("JobInfoRule encounter %s exception when calculating breakdown.", e.toString()));
            }



            return true;


        }
    }


    protected class JobNameDerivationRule extends DerivationRule {

        public JobNameDerivationRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
//
//
//            BasicInfo jobNameInfo = new BasicInfo("JobName");
//            jobNameInfo.addInfo("unspecified", new ArrayList<Source>());
//            job.addInfo(jobNameInfo);
//
//            String jobName = null;
//            List<Operation> operations = job.findOperations(OpenGType.MRApp, OpenGType.MRJob);
//            for (Operation operation : operations) {
//                jobName = operation.getInfo("JobName").getValue();
//            }
//            if(jobName == null) {
//                throw new IllegalStateException();
//            }

            platform.setName("A GraphMat job");
            platform.setType("GraphMat");

            return true;

        }
    }


    public BasicInfo basicInfo(String key, String value) {
        List<Source> sources = new ArrayList<>();
        BasicInfo info = new BasicInfo(key);
        info.setDescription("No description was set.");
        info.addInfo(value, sources);
        return info;
    }
}
