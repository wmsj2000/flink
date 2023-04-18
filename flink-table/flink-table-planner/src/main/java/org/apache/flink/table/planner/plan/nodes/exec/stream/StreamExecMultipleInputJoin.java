/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputJoinTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.multiway.MultipleInputStreamJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.multiway.ParallelismAndResourceOfMultipleInputJoin;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Stream {@link ExecNode} for multiple input join which contains a sub-graph of join {@link
 * ExecNode}s. The root node of the sub-graph is {@link #rootNode}, and the leaf nodes of the
 * sub-graph are the output nodes of the {@code getInputNodes()}.
 *
 * @author Quentin Qiu
 */
public class StreamExecMultipleInputJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    private final ExecNode<?> rootNode;
    private List<ExecEdge> originalEdges;

    public StreamExecMultipleInputJoin(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            ExecNode<?> rootNode,
            List<ExecEdge> originalEdges,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMultipleInputJoin.class),
                ExecNodeContext.newPersistedConfig(StreamExecMultipleInputJoin.class, tableConfig),
                inputProperties,
                rootNode.getOutputType(),
                description);
        this.rootNode = rootNode;
        checkArgument(inputProperties.size() == originalEdges.size());
        this.originalEdges = originalEdges;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        List<ExecEdge> inputEdges = getInputEdges();
        /*old input filed types*/
        List<RowType> oldRowTypes = new ArrayList<>();
        for (ExecEdge inputEdge : inputEdges) {
            oldRowTypes.add((RowType) inputEdge.getOutputType());
        }
        RowType outputType = (RowType) getOutputType();
        // get  input row type index according to output row type
        List<Integer> inputIndexOfOutput = getInputIndexOfOutput(oldRowTypes, outputType);
        // reorder inputs
        originalEdges = reorderList(originalEdges, inputIndexOfOutput);
        List<ExecEdge> reorderedInputEdges = reorderList(inputEdges, inputIndexOfOutput);
        final List<Transformation<?>> inputTransforms = new ArrayList<>();
        /*input filed types*/
        List<RowType> rowTypes = new ArrayList<>();
        /*warp serializer for fields*/
        List<InternalTypeInfo<RowData>> internalTypeInfos = new ArrayList<>();
        for (ExecEdge inputEdge : reorderedInputEdges) {
            RowType rowType = (RowType) inputEdge.getOutputType();
            rowTypes.add(rowType);
            internalTypeInfos.add(InternalTypeInfo.of(rowType));
            inputTransforms.add(inputEdge.translateToPlan(planner));
        }
        final Transformation<?> outputTransform = rootNode.translateToPlan(planner);
        // get join keys and upsertKeys of every input
        Pair<List<int[]>, List<List<int[]>>> joinKeysAndUpsertKeysPair =
                getJoinKeysAndUpsertKeysPair(originalEdges, reorderedInputEdges);
        List<int[]> joinKeys = joinKeysAndUpsertKeysPair.getKey();
        List<List<int[]>> upsertKeysList = joinKeysAndUpsertKeysPair.getValue();
        List<JoinInputSideSpec> joinInputSideSpecs = new ArrayList<>();
        for (int i = 0; i < originalEdges.size(); i++) {
            JoinInputSideSpec joinInputSideSpec =
                    JoinUtil.analyzeJoinInput(
                            planner.getFlinkContext().getClassLoader(),
                            internalTypeInfos.get(i),
                            joinKeys.get(i),
                            upsertKeysList.get(i));
            joinInputSideSpecs.add(joinInputSideSpec);
        }
        /*
         * get List<List<GeneratedJoinCondition>>,
         * any two input's condition can get by generatedJoinConditionsList.get[min(i,j)].get[max(j,)]
         */
        List<List<GeneratedJoinCondition>> generatedJoinConditionsList =
                getConditionsList(rowTypes, joinKeys, planner, config);
        // set KeyType and Selector for state
        List<RowDataKeySelector> selectors = new ArrayList<>();
        for (int i = 0; i < internalTypeInfos.size(); i++) {
            RowDataKeySelector selector =
                    KeySelectorUtil.getRowDataSelector(
                            planner.getFlinkContext().getClassLoader(),
                            joinKeys.get(i),
                            internalTypeInfos.get(i));
            selectors.add(selector);
        }
        MultipleInputStreamJoinOperatorFactory operatorFactory =
                new MultipleInputStreamJoinOperatorFactory(
                        getInputEdges().size(),
                        joinInputSideSpecs,
                        internalTypeInfos,
                        generatedJoinConditionsList,
                        config.getStateRetentionTime());
        final RowType returnType = (RowType) getOutputType();
        final MultipleInputJoinTransformation<RowData> multipleInputJoinTransform =
                new MultipleInputJoinTransformation<>(
                        createTransformationName(config),
                        operatorFactory,
                        InternalTypeInfo.of(returnType),
                        inputTransforms.get(0).getParallelism(),
                        false,
                        selectors.get(0).getProducedType());
        for (int i = 0; i < inputTransforms.size(); i++) {
            multipleInputJoinTransform.addInput(inputTransforms.get(i), selectors.get(i));
        }
        multipleInputJoinTransform.setDescription(createTransformationDescription(config));
        // calculate parallelism and resource for multiple input join node
        final ParallelismAndResourceOfMultipleInputJoin calculator =
                new ParallelismAndResourceOfMultipleInputJoin(inputTransforms, outputTransform);
        calculator.calculate();
        // set maxParallelism
        if (calculator.getMaxParallelism() > 0) {
            multipleInputJoinTransform.setMaxParallelism(calculator.getMaxParallelism());
        }
        // set resources
        multipleInputJoinTransform.setResources(
                calculator.getMinResources(), calculator.getPreferredResources());
        final int memoryWeight = calculator.getManagedMemoryWeight();

        final long memoryBytes = (long) memoryWeight << 20;
        ExecNodeUtil.setManagedMemoryWeight(multipleInputJoinTransform, memoryBytes);

        // set chaining strategy for source chaining
        multipleInputJoinTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        return multipleInputJoinTransform;
    }

    private <T> List<T> reorderList(List<T> oldList, List<Integer> newIndexList) {
        List<T> newList = new ArrayList<>(Collections.nCopies(oldList.size(), null));
        for (int i = 0; i < newIndexList.size(); i++) {
            newList.set(newIndexList.get(i), oldList.get(i));
        }
        return newList;
    }

    private List<Integer> getInputIndexOfOutput(List<RowType> rowTypes, RowType outputType) {
        List<List<LogicalType>> inputRowTypes = new ArrayList<>();
        for (RowType rowType : rowTypes) {
            List<LogicalType> inputRowType = new ArrayList<>();
            for (RowType.RowField field : rowType.getFields()) {
                inputRowType.add(field.getType());
            }
            inputRowTypes.add(inputRowType);
        }
        int[] visited = new int[inputRowTypes.size()];
        List<Integer> indexes = new ArrayList<>();
        List<LogicalType> outputRowType = new ArrayList<>();
        for (RowType.RowField field : outputType.getFields()) {
            outputRowType.add(field.getType());
        }
        int current = 0;
        while (current < outputRowType.size()) {
            int skip = 1;
            for (int i = 0; i < inputRowTypes.size(); i++) {
                if (visited[i] != 1
                        && outputRowType
                                .subList(current, current + inputRowTypes.get(i).size())
                                .equals(inputRowTypes.get(i))) {
                    visited[i] = 1;
                    skip = inputRowTypes.get(i).size();
                    indexes.add(i);
                    break;
                }
            }
            current += skip;
        }
        List<Integer> newIndexes = new ArrayList<>();
        for (int i = 0; i < indexes.size(); i++) {
            newIndexes.add(indexes.indexOf(i));
        }
        return newIndexes;
    }

    private Pair<List<int[]>, List<List<int[]>>> getJoinKeysAndUpsertKeysPair(
            List<ExecEdge> originalEdges, List<ExecEdge> inputEdges) {
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (ExecEdge edge : inputEdges) {
            inputNodes.add(edge.getSource());
        }
        List<int[]> joinKeysList = new ArrayList<>(Collections.nCopies(inputEdges.size(), null));
        List<List<int[]>> upsertKeysList =
                new ArrayList<>(Collections.nCopies(inputEdges.size(), null));
        for (ExecEdge originalEdge : originalEdges) {
            int inputIndex = inputNodes.indexOf(originalEdge.getSource());
            ExecNode<?> targetNode = originalEdge.getTarget();
            if (!(targetNode instanceof StreamExecJoin)) {
                throw new RuntimeException("targetNode of originalEdge must be StreamExecJoin");
            }
            if (targetNode.getInputEdges().get(0) == originalEdge) {
                joinKeysList.set(
                        inputIndex, ((StreamExecJoin) targetNode).getJoinSpec().getLeftKeys());
                upsertKeysList.set(inputIndex, ((StreamExecJoin) targetNode).getLeftUpsertKeys());
            } else if (targetNode.getInputEdges().get(1) == originalEdge) {
                joinKeysList.set(
                        inputIndex, ((StreamExecJoin) targetNode).getJoinSpec().getRightKeys());
                upsertKeysList.set(inputIndex, ((StreamExecJoin) targetNode).getRightUpsertKeys());
            } else {
                throw new RuntimeException("originalEdge must be targetNode's inputEdge");
            }
        }
        return Pair.of(joinKeysList, upsertKeysList);
    }

    private List<List<GeneratedJoinCondition>> getConditionsList(
            List<RowType> rowTypes,
            List<int[]> joinKeys,
            PlannerBase planner,
            ExecNodeConfig config) {
        List<List<GeneratedJoinCondition>> joinConditionsList = new ArrayList<>();
        for (int i = 0; i < originalEdges.size(); i++) {
            List<GeneratedJoinCondition> joinConditions = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                JoinSpec joinSpec =
                        new JoinSpec(
                                FlinkJoinType.INNER,
                                joinKeys.get(j),
                                joinKeys.get(i),
                                new boolean[] {true},
                                null);
                GeneratedJoinCondition generatedCondition =
                        JoinUtil.generateConditionFunction(
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                joinSpec,
                                rowTypes.get(j),
                                rowTypes.get(i));
                joinConditions.add(generatedCondition);
            }
            joinConditionsList.add(joinConditions);
        }
        return joinConditionsList;
    }
}
