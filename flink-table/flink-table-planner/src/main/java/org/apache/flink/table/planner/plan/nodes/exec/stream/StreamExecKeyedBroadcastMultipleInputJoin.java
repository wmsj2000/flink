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
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.KeyedBroadcastStateMultipleInputTransformation;
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
import org.apache.flink.table.runtime.operators.join.multiway.KeyedBroadcastMultipleInputStreamJoinOperatorFactory;
import org.apache.flink.table.runtime.operators.join.multiway.MultipleInputJoinEdge;
import org.apache.flink.table.runtime.operators.join.multiway.ParallelismAndResourceOfMultipleInputJoin;
import org.apache.flink.table.runtime.operators.join.stream.state.MultipleInputJoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.table.planner.plan.nodes.exec.InputProperty.BROADCAST_DISTRIBUTION;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Stream {@link ExecNode} for multiple input join which contains a sub-graph of join {@link
 * ExecNode}s. The root node of the sub-graph is {@link #rootNode}, and the leaf nodes of the
 * sub-graph are the output nodes of the {@code getInputNodes()}.
 *
 * @author Quentin Qiu
 */
public class StreamExecKeyedBroadcastMultipleInputJoin<R> extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    private final ExecNode<?> rootNode;
    private List<ExecEdge> originalEdges;

    public StreamExecKeyedBroadcastMultipleInputJoin(
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
        originalEdges = getSortedEdges(rootNode);
        List<ExecEdge> reorderedInputEdges = reorderInputEdges(inputEdges, originalEdges);
        List<RowType> rowTypes = new ArrayList<>();
        List<InternalTypeInfo<RowData>> internalTypeInfos = new ArrayList<>();
        for (ExecEdge inputEdge : reorderedInputEdges) {
            RowType rowType = (RowType) inputEdge.getOutputType();
            rowTypes.add(rowType);
            internalTypeInfos.add(InternalTypeInfo.of(rowType));
        }
        List<List<int[]>> upsertKeyLists = getUpsertKeyLists();
        MultipleInputJoinEdge[][] multipleInputJoinEdges =
                getMultipleInputJoinEdgesV2(
                        planner, config, upsertKeyLists, internalTypeInfos, rowTypes);
        List<RowDataKeySelector> outputSelectors =
                getInputSelectorsForOutput(planner, internalTypeInfos);
        List<MultipleInputJoinInputSideSpec> multipleInputJoinInputSideSpecs = new ArrayList<>();
        for (int i = 0; i < originalEdges.size(); i++) {
            boolean isBroadcastSide = i > 1;
            Pair<List<int[]>, List<RowDataKeySelector>> joinKeysAndSelectorsPair =
                    getJoinKeyAndSelectorsList(
                            planner, multipleInputJoinEdges[i], internalTypeInfos.get(i));
            MultipleInputJoinInputSideSpec multipleInputJoinInputSideSpec =
                    new MultipleInputJoinInputSideSpec(
                            isBroadcastSide,
                            internalTypeInfos.get(i),
                            i,
                            outputSelectors.get(i),
                            joinKeysAndSelectorsPair.getLeft(),
                            joinKeysAndSelectorsPair.getRight());
            multipleInputJoinInputSideSpec.analyzeMultipleInputJoinInput(multipleInputJoinEdges);
            multipleInputJoinInputSideSpecs.add(multipleInputJoinInputSideSpec);
        }
        // change shuffle
        changeExechangeNodeDistribution(reorderedInputEdges, multipleInputJoinInputSideSpecs);
        final List<Transformation<?>> inputTransforms = new ArrayList<>();
        for (ExecEdge inputEdge : reorderedInputEdges) {
            inputTransforms.add(inputEdge.translateToPlan(planner));
        }
        // final Transformation<?> outputTransform = rootNode.translateToPlan(planner);
        // set selectors for state
        List<RowDataKeySelector> selectors = new ArrayList<>();
        for (int i = 0; i < originalEdges.size(); i++) {
            selectors.add(multipleInputJoinInputSideSpecs.get(i).getStateKeySelector());
        }

        KeyedBroadcastMultipleInputStreamJoinOperatorFactory operatorFactory =
                new KeyedBroadcastMultipleInputStreamJoinOperatorFactory(
                        getInputEdges().size(),
                        multipleInputJoinInputSideSpecs,
                        internalTypeInfos,
                        multipleInputJoinEdges,
                        config.getStateRetentionTime());
        final RowType returnType = (RowType) getOutputType();
        final KeyedBroadcastStateMultipleInputTransformation<RowData> multipleInputJoinTransform =
                new KeyedBroadcastStateMultipleInputTransformation<>(
                        createTransformationName(config),
                        operatorFactory,
                        InternalTypeInfo.of(returnType),
                        inputTransforms.get(0).getParallelism(),
                        selectors.get(0).getProducedType());
        // set 2 selectors for 2 keyed stream
        for (int i = 0; i < inputTransforms.size(); i++) {
            RowDataKeySelector selector = i < 2 ? selectors.get(i) : null;
            multipleInputJoinTransform.addInput(inputTransforms.get(i), selector);
        }
        multipleInputJoinTransform.setDescription(createTransformationDescription(config));
        // setMultipleInputJoinTransform(multipleInputJoinTransform, inputTransforms,
        // outputTransform);
        return multipleInputJoinTransform;
    }

    private List<ExecEdge> reorderInputEdges(
            List<ExecEdge> inputEdges, List<ExecEdge> originalEdges) {
        List<ExecEdge> reorderedInputEdges =
                new ArrayList<>(Collections.nCopies(inputEdges.size(), null));
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (ExecEdge originalEdge : originalEdges) {
            inputNodes.add(originalEdge.getSource());
        }
        for (ExecEdge inputEdge : inputEdges) {
            int newIndex = inputNodes.indexOf(inputEdge.getSource());
            reorderedInputEdges.set(newIndex, inputEdge);
        }
        return reorderedInputEdges;
    }

    private List<ExecEdge> getSortedEdges(ExecNode<?> rootNode) {
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (ExecEdge edge : originalEdges) {
            inputNodes.add(edge.getSource());
        }
        List<ExecEdge> sortedEdges = new ArrayList<>();
        dfsSort(rootNode, sortedEdges, inputNodes);
        return sortedEdges;
    }

    private void dfsSort(
            ExecNode<?> rootNode, List<ExecEdge> sortedEdges, List<ExecNode<?>> inputNodes) {
        if (inputNodes.contains(rootNode)) {
            return;
        }
        for (ExecEdge edge : rootNode.getInputEdges()) {
            ExecNode<?> node = edge.getSource();
            dfsSort(node, sortedEdges, inputNodes);
            if (originalEdges.contains(edge)) {
                sortedEdges.add(edge);
            }
        }
    }

    private void changeExechangeNodeDistribution(
            List<ExecEdge> inputEdges,
            List<MultipleInputJoinInputSideSpec> multipleInputJoinInputSideSpecs) {
        for (int i = 0; i < inputEdges.size(); i++) {
            ExecEdge inputEdge = inputEdges.get(i);
            checkArgument(inputEdge.getSource() instanceof StreamExecExchange);
            InputProperty inputProperty = inputEdge.getSource().getInputProperties().get(0);
            boolean isBroadcast = i > 1;
            if (isBroadcast) {

                inputProperty.setRequiredDistribution(BROADCAST_DISTRIBUTION);
                ((StreamExecExchange) inputEdge.getSource())
                        .setDescription("distribution=[broadcast]");
            } else {
                if (inputProperty.getRequiredDistribution()
                        instanceof InputProperty.HashDistribution) {
                    int[] keys = multipleInputJoinInputSideSpecs.get(i).getStateKey();
                    ((InputProperty.HashDistribution) inputProperty.getRequiredDistribution())
                            .setKeys(keys);
                }
            }
            // inputEdge.getSource().
        }
    }

    private Pair<List<int[]>, List<RowDataKeySelector>> getJoinKeyAndSelectorsList(
            PlannerBase planner,
            MultipleInputJoinEdge[] edges,
            InternalTypeInfo<RowData> internalTypeInfo) {
        List<int[]> joinKeyList = new ArrayList<>(Collections.nCopies(edges.length, null));
        List<RowDataKeySelector> selectorList =
                new ArrayList<>(Collections.nCopies(edges.length, null));
        for (int i = 0; i < edges.length; i++) {
            MultipleInputJoinEdge edge = edges[i];
            if (edge == null) {
                continue;
            }
            joinKeyList.set(i, edge.getLeftJoinKey());
            selectorList.set(i, edge.getLeftJoinKeySelector());
        }
        return Pair.of(joinKeyList, selectorList);
    }

    private void setMultipleInputJoinTransform(
            AbstractMultipleInputTransformation<RowData> multipleInputJoinTransform,
            List<Transformation<?>> inputTransforms,
            Transformation<?> outputTransform) {
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
    }

    private MultipleInputJoinEdge[][] getMultipleInputJoinEdges(
            PlannerBase planner,
            ExecNodeConfig config,
            List<List<int[]>> upsertKeyLists,
            List<InternalTypeInfo<RowData>> internalTypeInfos,
            List<RowType> rowTypes) {
        int numberOfInputs = originalEdges.size();
        MultipleInputJoinEdge[][] joinEdges =
                new MultipleInputJoinEdge[numberOfInputs][numberOfInputs];
        List<ExecNode<?>> originalNodes = getOriginalNodes();
        for (ExecNode<?> node : originalNodes) {
            if (!(node instanceof StreamExecJoin)) {
                continue;
            }
            int[] leftKeys = ((StreamExecJoin) node).getJoinSpec().getLeftKeys();
            int[] rightKeys = ((StreamExecJoin) node).getJoinSpec().getRightKeys();
            Pair<Integer, int[]> leftInputJoinKeyPair =
                    getJoinInputIndexWithCal(node.getInputEdges().get(0), leftKeys);
            Pair<Integer, int[]> rightInpuJoinKeytPair =
                    getJoinInputIndexWithCal(node.getInputEdges().get(1), rightKeys);
            assert leftInputJoinKeyPair != null;
            assert rightInpuJoinKeytPair != null;
            int inputIndex1 = leftInputJoinKeyPair.getLeft();
            int inputIndex2 = rightInpuJoinKeytPair.getLeft();
            int[] joinKey1 = leftInputJoinKeyPair.getRight();
            int[] joinKey2 = rightInpuJoinKeytPair.getRight();
            RowDataKeySelector joinKeySelector1 =
                    KeySelectorUtil.getRowDataSelector(
                            planner.getFlinkContext().getClassLoader(),
                            joinKey1,
                            internalTypeInfos.get(inputIndex1));
            RowDataKeySelector joinKeySelector2 =
                    KeySelectorUtil.getRowDataSelector(
                            planner.getFlinkContext().getClassLoader(),
                            joinKey2,
                            internalTypeInfos.get(inputIndex2));
            JoinSpec joinSpec1 =
                    new JoinSpec(
                            ((StreamExecJoin) node).getJoinSpec().getJoinType(),
                            joinKey1,
                            joinKey2,
                            ((StreamExecJoin) node).getJoinSpec().getFilterNulls(),
                            null);
            GeneratedJoinCondition generatedCondition1 =
                    JoinUtil.generateConditionFunction(
                            config,
                            planner.getFlinkContext().getClassLoader(),
                            joinSpec1,
                            rowTypes.get(inputIndex1),
                            rowTypes.get(inputIndex2));
            joinEdges[inputIndex1][inputIndex2] =
                    new MultipleInputJoinEdge(
                            generatedCondition1,
                            ((StreamExecJoin) node).getJoinSpec().getJoinType(),
                            inputIndex1,
                            inputIndex2,
                            joinKey1,
                            joinKey2,
                            upsertKeyLists.get(inputIndex1),
                            upsertKeyLists.get(inputIndex2),
                            internalTypeInfos.get(inputIndex1),
                            internalTypeInfos.get(inputIndex2),
                            joinKeySelector1,
                            joinKeySelector2,
                            ((StreamExecJoin) node).getJoinSpec().getFilterNulls());
            JoinSpec joinSpec2 =
                    new JoinSpec(
                            ((StreamExecJoin) node).getJoinSpec().getJoinType(),
                            joinKey2,
                            joinKey1,
                            ((StreamExecJoin) node).getJoinSpec().getFilterNulls(),
                            null);
            GeneratedJoinCondition generatedCondition2 =
                    JoinUtil.generateConditionFunction(
                            config,
                            planner.getFlinkContext().getClassLoader(),
                            joinSpec2,
                            rowTypes.get(inputIndex2),
                            rowTypes.get(inputIndex1));
            joinEdges[inputIndex2][inputIndex1] =
                    new MultipleInputJoinEdge(
                            generatedCondition2,
                            ((StreamExecJoin) node).getJoinSpec().getJoinType(),
                            inputIndex2,
                            inputIndex1,
                            joinKey2,
                            joinKey1,
                            upsertKeyLists.get(inputIndex2),
                            upsertKeyLists.get(inputIndex1),
                            internalTypeInfos.get(inputIndex2),
                            internalTypeInfos.get(inputIndex1),
                            joinKeySelector2,
                            joinKeySelector1,
                            ((StreamExecJoin) node).getJoinSpec().getFilterNulls());
        }
        return joinEdges;
    }

    private MultipleInputJoinEdge[][] getMultipleInputJoinEdgesV2(
            PlannerBase planner,
            ExecNodeConfig config,
            List<List<int[]>> upsertKeyLists,
            List<InternalTypeInfo<RowData>> internalTypeInfos,
            List<RowType> rowTypes) {
        int numberOfInputs = originalEdges.size();
        MultipleInputJoinEdge[][] joinEdges =
                new MultipleInputJoinEdge[numberOfInputs][numberOfInputs];
        List<ExecNode<?>> originalNodes = getOriginalNodes();
        for (ExecNode<?> node : originalNodes) {
            if (!(node instanceof StreamExecJoin)) {
                continue;
            }
            int[] leftKeys = ((StreamExecJoin) node).getJoinSpec().getLeftKeys();
            int[] rightKeys = ((StreamExecJoin) node).getJoinSpec().getRightKeys();
            boolean[] filterNulls = ((StreamExecJoin) node).getJoinSpec().getFilterNulls();
            FlinkJoinType joinType = ((StreamExecJoin) node).getJoinSpec().getJoinType();
            for (int i = 0; i < leftKeys.length; i++) {
                Pair<Integer, int[]> leftInputJoinKeyPair =
                        getJoinInputIndexWithCal(
                                node.getInputEdges().get(0), new int[] {leftKeys[i]});
                Pair<Integer, int[]> rightInpuJoinKeytPair =
                        getJoinInputIndexWithCal(
                                node.getInputEdges().get(1), new int[] {rightKeys[i]});
                assert leftInputJoinKeyPair != null;
                assert rightInpuJoinKeytPair != null;
                int inputIndex1 = leftInputJoinKeyPair.getLeft();
                int inputIndex2 = rightInpuJoinKeytPair.getLeft();
                int[] joinKey1 = leftInputJoinKeyPair.getRight();
                int[] joinKey2 = rightInpuJoinKeytPair.getRight();
                boolean[] filterNullNew = new boolean[] {filterNulls[i]};
                if (joinEdges[inputIndex1][inputIndex2] != null) {
                    int[] joinKeyLeft = joinEdges[inputIndex1][inputIndex2].getLeftJoinKey();
                    int[] joinKeyRight = joinEdges[inputIndex1][inputIndex2].getRightJoinKey();
                    boolean[] filterNullTmp = joinEdges[inputIndex1][inputIndex2].getFilterNulls();
                    joinKey1 = mergeArray(joinKeyLeft, joinKey1);
                    joinKey2 = mergeArray(joinKeyRight, joinKey2);
                    filterNullNew = mergeBooleanArray(filterNullTmp, filterNullNew);
                }
                RowDataKeySelector joinKeySelector1 =
                        KeySelectorUtil.getRowDataSelector(
                                planner.getFlinkContext().getClassLoader(),
                                joinKey1,
                                internalTypeInfos.get(inputIndex1));
                RowDataKeySelector joinKeySelector2 =
                        KeySelectorUtil.getRowDataSelector(
                                planner.getFlinkContext().getClassLoader(),
                                joinKey2,
                                internalTypeInfos.get(inputIndex2));
                JoinSpec joinSpec1 =
                        new JoinSpec(joinType, joinKey1, joinKey2, filterNullNew, null);
                GeneratedJoinCondition generatedCondition1 =
                        JoinUtil.generateConditionFunction(
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                joinSpec1,
                                rowTypes.get(inputIndex1),
                                rowTypes.get(inputIndex2));
                joinEdges[inputIndex1][inputIndex2] =
                        new MultipleInputJoinEdge(
                                generatedCondition1,
                                joinType,
                                inputIndex1,
                                inputIndex2,
                                joinKey1,
                                joinKey2,
                                upsertKeyLists.get(inputIndex1),
                                upsertKeyLists.get(inputIndex2),
                                internalTypeInfos.get(inputIndex1),
                                internalTypeInfos.get(inputIndex2),
                                joinKeySelector1,
                                joinKeySelector2,
                                filterNullNew);
                JoinSpec joinSpec2 =
                        new JoinSpec(joinType, joinKey2, joinKey1, filterNullNew, null);
                GeneratedJoinCondition generatedCondition2 =
                        JoinUtil.generateConditionFunction(
                                config,
                                planner.getFlinkContext().getClassLoader(),
                                joinSpec2,
                                rowTypes.get(inputIndex2),
                                rowTypes.get(inputIndex1));
                joinEdges[inputIndex2][inputIndex1] =
                        new MultipleInputJoinEdge(
                                generatedCondition2,
                                joinType,
                                inputIndex2,
                                inputIndex1,
                                joinKey2,
                                joinKey1,
                                upsertKeyLists.get(inputIndex2),
                                upsertKeyLists.get(inputIndex1),
                                internalTypeInfos.get(inputIndex2),
                                internalTypeInfos.get(inputIndex1),
                                joinKeySelector2,
                                joinKeySelector1,
                                filterNullNew);
            }
        }
        return joinEdges;
    }

    private boolean[] mergeBooleanArray(boolean[] arr1, boolean[] arr2) {
        if (arr1 == null && arr2 == null) {
            return null;
        } else if (arr1 == null) {
            return Arrays.copyOfRange(arr2, 0, arr2.length);
        } else if (arr2 == null) {
            return Arrays.copyOfRange(arr1, 0, arr1.length);
        }
        boolean[] result = new boolean[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, result, 0, arr1.length);
        System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
        return result;
    }

    private int[] mergeArray(int[] arr1, int[] arr2) {
        if (arr1 == null && arr2 == null) {
            return null;
        } else if (arr1 == null) {
            return Arrays.copyOfRange(arr2, 0, arr2.length);
        } else if (arr2 == null) {
            return Arrays.copyOfRange(arr1, 0, arr1.length);
        }
        int[] result = new int[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, result, 0, arr1.length);
        System.arraycopy(arr2, 0, result, arr1.length, arr2.length);
        return result;
    }

    private Pair<Integer, int[]> getJoinInputIndexWithCal(ExecEdge execEdge, int[] keyIndexs) {
        if (originalEdges.contains(execEdge)) {
            int inputIndex = originalEdges.indexOf(execEdge);
            return Pair.of(inputIndex, keyIndexs);
        }
        if (execEdge.getSource() instanceof StreamExecExchange) {
            execEdge = execEdge.getSource().getInputEdges().get(0);
        }
        if (execEdge.getSource() instanceof StreamExecCalc) {
            execEdge = execEdge.getSource().getInputEdges().get(0);
            checkArgument(execEdge.getSource() instanceof StreamExecJoin);
            StreamExecCalc calNode = (StreamExecCalc) execEdge.getTarget();
            List<RexNode> projection = calNode.getProjection();
            List<Integer> indexs = new ArrayList<>();
            for (Object obj : projection) {
                RexInputRef ref = (RexInputRef) obj;
                indexs.add(ref.getIndex());
            }
            for (int i = 0; i < keyIndexs.length; i++) {
                keyIndexs[i] = indexs.get(keyIndexs[i]);
            }
        }
        if (execEdge.getSource() instanceof StreamExecJoin) {
            ExecEdge preLeftEdge = execEdge.getSource().getInputEdges().get(0);
            ExecEdge preRightEdge = execEdge.getSource().getInputEdges().get(1);
            RowType preLeftOutputType = (RowType) preLeftEdge.getOutputType();
            RowType preRightOutputType = (RowType) preRightEdge.getOutputType();
            int leftSize = preLeftOutputType.getFields().size();
            int rightSize = preRightOutputType.getFields().size();
            if (allLessThan(keyIndexs, 0) || allMoreThan(keyIndexs, leftSize + rightSize)) {
                throw new RuntimeException("join key out of bound");
            }
            boolean keysInLeft = allLessThan(keyIndexs, leftSize);
            boolean keysInRight = allMoreThan(keyIndexs, leftSize);
            if (!(keysInLeft || keysInRight)) {
                throw new RuntimeException("join keys should come from single input");
            }
            if (keysInLeft) {
                return getJoinInputIndexWithCal(preLeftEdge, keyIndexs);
            } else {
                int[] newIndexs = minus(keyIndexs, leftSize);
                return getJoinInputIndexWithCal(preRightEdge, newIndexs);
            }
        } else {
            throw new RuntimeException("mj group only contains exchange or join or cal");
        }
    }

    private List<RowDataKeySelector> getInputSelectorsForOutput(
            PlannerBase planner, List<InternalTypeInfo<RowData>> internalTypeInfos) {
        checkArgument(rootNode instanceof StreamExecJoin);
        ExecEdge preLeftEdge = rootNode.getInputEdges().get(0);
        ExecEdge preRightEdge = rootNode.getInputEdges().get(1);
        RowType preLeftOutputType = (RowType) preLeftEdge.getOutputType();
        RowType preRightOutputType = (RowType) preRightEdge.getOutputType();
        int leftSize = preLeftOutputType.getFields().size();
        int rightSize = preRightOutputType.getFields().size();
        RowType rowType = (RowType) rootNode.getOutputType();
        InternalTypeInfo<RowData> internalTypeInfo = InternalTypeInfo.of(rowType);
        List<List<Integer>> inputKeyIndexlists = new ArrayList<>();
        for (int i = 0; i < originalEdges.size(); i++) {
            inputKeyIndexlists.add(new ArrayList<>());
        }
        for (int i = 0; i < leftSize + rightSize; i++) {
            Pair<Integer, int[]> pair;
            if (i < leftSize) {
                pair = getJoinInputIndexWithCal(preLeftEdge, new int[] {i});
            } else {
                pair = getJoinInputIndexWithCal(preRightEdge, new int[] {i - leftSize});
            }
            int inputIndex = pair.getLeft();
            int[] keyIndex = pair.getRight();
            inputKeyIndexlists.get(inputIndex).add(keyIndex[0]);
        }
        List<RowDataKeySelector> selectors = new ArrayList<>();
        for (int i = 0; i < inputKeyIndexlists.size(); i++) {
            List<Integer> inputKeyIndexlist = inputKeyIndexlists.get(i);
            RowDataKeySelector joinKeySelector =
                    KeySelectorUtil.getRowDataSelector(
                            planner.getFlinkContext().getClassLoader(),
                            inputKeyIndexlist.stream().mapToInt(Integer::valueOf).toArray(),
                            internalTypeInfos.get(i));
            selectors.add(joinKeySelector);
        }
        return selectors;
    }

    private List<List<int[]>> getUpsertKeyLists() {
        List<List<int[]>> upsertKeysList =
                new ArrayList<>(Collections.nCopies(originalEdges.size(), null));
        for (ExecEdge originalEdge : originalEdges) {
            int inputIndex = originalEdges.indexOf(originalEdge);
            ExecNode<?> targetNode = originalEdge.getTarget();
            if (!(targetNode instanceof StreamExecJoin)) {
                throw new RuntimeException("targetNode of originalEdge must be StreamExecJoin");
            }
            if (targetNode.getInputEdges().get(0) == originalEdge) {
                upsertKeysList.set(inputIndex, ((StreamExecJoin) targetNode).getLeftUpsertKeys());
            } else if (targetNode.getInputEdges().get(1) == originalEdge) {
                upsertKeysList.set(inputIndex, ((StreamExecJoin) targetNode).getRightUpsertKeys());
            } else {
                throw new RuntimeException("originalEdge must be targetNode's inputEdge");
            }
        }
        return upsertKeysList;
    }

    private List<ExecNode<?>> getOriginalNodes() {
        List<ExecNode<?>> result = new ArrayList<>();
        Queue<ExecNode<?>> queue = new LinkedList<>();
        queue.offer(rootNode);
        while (!queue.isEmpty()) {
            ExecNode<?> node = queue.poll();
            result.add(node);
            for (ExecEdge inputEdge : node.getInputEdges()) {
                if (!originalEdges.contains(inputEdge)) {
                    queue.offer(inputEdge.getSource());
                }
            }
        }
        return result;
    }

    public static boolean allLessThan(int[] arr, int n) {
        for (int j : arr) {
            if (j >= n) {
                return false;
            }
        }
        return true;
    }

    public static boolean allMoreThan(int[] arr, int n) {
        for (int j : arr) {
            if (j < n) {
                return false;
            }
        }
        return true;
    }

    public int[] minus(int[] nums, int t) {
        int n = nums.length;
        for (int i = 0; i < n; i++) {
            nums[i] = nums[i] - t;
        }
        return nums;
    }
}
