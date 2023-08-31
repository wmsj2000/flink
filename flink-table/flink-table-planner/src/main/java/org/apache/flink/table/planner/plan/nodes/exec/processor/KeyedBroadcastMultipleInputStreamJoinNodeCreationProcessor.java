/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecKeyedBroadcastMultipleInputJoin;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A {@link ExecNodeGraphProcessor} which organize {@link ExecNode}s into multiple input join nodes.
 *
 * @author Quentin Qiu
 */
public class KeyedBroadcastMultipleInputStreamJoinNodeCreationProcessor
        implements ExecNodeGraphProcessor {

    public KeyedBroadcastMultipleInputStreamJoinNodeCreationProcessor() {}

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        List<ExecNodeWrapper> rootWrappers = wrapExecNodes(execGraph.getRootNodes());
        // sort all nodes in topological order, sinks come first and sources come last
        List<ExecNodeWrapper> orderedWrappers = topologicalSort(rootWrappers);
        // group nodes into multiple input join groups
        createMultipleInputJoinGroups(orderedWrappers);
        // apply optimizations to remove unnecessary nodes out of multiple input join groups
        optimizeMultipleInputJoinGroups(orderedWrappers);
        // create the real multiple input nodes
        List<ExecNode<?>> newRootNodes =
                createMultipleInputJoinNodes(context.getPlanner().getTableConfig(), rootWrappers);
        return new ExecNodeGraph(newRootNodes);
    }
    // --------------------------------------------------------------------------------
    // Wrapping and Sorting
    // --------------------------------------------------------------------------------

    private List<ExecNodeWrapper> wrapExecNodes(List<ExecNode<?>> rootNodes) {
        Map<ExecNode<?>, ExecNodeWrapper> wrapperMap = new HashMap<>();
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        ExecNodeWrapper wrapper =
                                wrapperMap.computeIfAbsent(node, k -> new ExecNodeWrapper(node));
                        for (ExecEdge inputEdge : node.getInputEdges()) {
                            ExecNode<?> inputNode = inputEdge.getSource();
                            ExecNodeWrapper inputWrapper =
                                    wrapperMap.computeIfAbsent(
                                            inputNode, k -> new ExecNodeWrapper(inputNode));
                            wrapper.inputs.add(inputWrapper);
                            inputWrapper.outputs.add(wrapper);
                        }
                        visitInputs(node);
                    }
                };
        rootNodes.forEach(s -> s.accept(visitor));

        List<ExecNodeWrapper> rootWrappers = new ArrayList<>();
        for (ExecNode<?> root : rootNodes) {
            ExecNodeWrapper rootWrapper = wrapperMap.get(root);
            Preconditions.checkNotNull(rootWrapper, "Root node is not wrapped. This is a bug.");
            rootWrappers.add(rootWrapper);
        }
        return rootWrappers;
    }

    private List<ExecNodeWrapper> topologicalSort(List<ExecNodeWrapper> rootWrappers) {
        List<ExecNodeWrapper> result = new ArrayList<>();
        Queue<ExecNodeWrapper> queue = new LinkedList<>(rootWrappers);
        Map<ExecNodeWrapper, Integer> visitCountMap = new HashMap<>();

        while (!queue.isEmpty()) {
            ExecNodeWrapper wrapper = queue.poll();
            result.add(wrapper);
            for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
                int visitCount =
                        visitCountMap.compute(inputWrapper, (k, v) -> v == null ? 1 : v + 1);
                if (visitCount == inputWrapper.outputs.size()) {
                    queue.offer(inputWrapper);
                }
            }
        }

        return result;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Join Groups Creating
    // --------------------------------------------------------------------------------

    private void createMultipleInputJoinGroups(List<ExecNodeWrapper> orderedWrappers) {
        // wrappers are checked in topological order from sinks to sources
        for (ExecNodeWrapper wrapper : orderedWrappers) {
            // only exchange and join can be multiple input join member
            if (!canBeMultipleInputJoinNodeMember(wrapper)) {
                continue;
            }

            // we first try to assign this wrapper into the same group with its outputs
            MultipleInputJoinGroup outputGroup = canBeInSameGroupWithOutputs(wrapper);
            if (outputGroup != null) {
                outputGroup.addMember(wrapper);
                continue;
            }

            // we then try to create a new multiple input group with this node as the root
            if (canBeRootOfMultipleInputJoinGroup(wrapper)) {
                wrapper.group = new MultipleInputJoinGroup(wrapper);
            }

            // all our attempts failed, this node will not be in a multiple input node
        }
    }

    private boolean canBeMultipleInputJoinNodeMember(ExecNodeWrapper wrapper) {
        if (wrapper.execNode instanceof CommonExecExchange){
            return true;
        }
        if(wrapper.execNode instanceof StreamExecJoin) {
            JoinSpec joinSpec = ((StreamExecJoin) wrapper.execNode).getJoinSpec();
            if(joinSpec.getJoinType()== FlinkJoinType.INNER && !joinSpec
                    .getNonEquiCondition()
                    .isPresent()){
                return true;
            }
        }
        if (wrapper.execNode instanceof StreamExecCalc
                && wrapper.execNode.getInputEdges().get(0).getSource() instanceof StreamExecJoin
                && ((StreamExecCalc) wrapper.execNode).getCondition() == null) {
            return true;
        }
        return false;
    }

    /**
     * A node can only be assigned into the same multiple input group of its outputs if all outputs
     * have a group and are the same.
     *
     * @return the {@link MultipleInputJoinGroup} of the outputs if all outputs have a group and are
     *     the same, null otherwise
     */
    private MultipleInputJoinGroup canBeInSameGroupWithOutputs(ExecNodeWrapper wrapper) {
        if (wrapper.outputs.isEmpty()) {
            return null;
        }

        MultipleInputJoinGroup outputGroup = wrapper.outputs.get(0).group;
        if (outputGroup == null) {
            return null;
        }

        for (ExecNodeWrapper outputWrapper : wrapper.outputs) {
            if (outputWrapper.group != outputGroup) {
                return null;
            }
        }
        return outputGroup;
    }

    private boolean canBeRootOfMultipleInputJoinGroup(ExecNodeWrapper wrapper) {
        // only a join node can be the root
        return wrapper.execNode instanceof StreamExecJoin;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Join Groups Optimizing
    // --------------------------------------------------------------------------------

    private void optimizeMultipleInputJoinGroups(List<ExecNodeWrapper> orderedWrappers) {
        // wrappers are checked in topological order from sources to sinks
        for (int i = orderedWrappers.size() - 1; i >= 0; i--) {
            ExecNodeWrapper wrapper = orderedWrappers.get(i);
            MultipleInputJoinGroup group = wrapper.group;
            if (group == null) {
                // we only consider nodes currently in a multiple input group
                continue;
            }
            if (!isEntranceOfMultipleInputJoinGroup(wrapper)) {
                // we're not removing a node from the middle of a multiple input group
                continue;
            }
            boolean shouldRemove = wrapper.execNode instanceof CommonExecExchange;
            if (shouldRemove) {
                wrapper.group.removeMember(wrapper);
            }
        }
        // number of join node in multiple input join group should be more than 3,if not,set group
        // to null
        for (ExecNodeWrapper wrapper : orderedWrappers) {
            if (wrapper.group != null && wrapper.group.getJoinCount() < 2) {
                for (ExecNodeWrapper w : wrapper.group.members) {
                    wrapper.group = null;
                }
            }
        }
    }

    private boolean isEntranceOfMultipleInputJoinGroup(ExecNodeWrapper wrapper) {
        Preconditions.checkNotNull(
                wrapper.group,
                "Exec node wrapper does not have a multiple input group. This is a bug.");
        for (ExecNodeWrapper inputWrapper : wrapper.inputs) {
            if (inputWrapper.group == wrapper.group) {
                // one of the input is in the same group, so this node is not the entrance of the
                // group
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------
    // Multiple Input Nodes Creating
    // --------------------------------------------------------------------------------

    private List<ExecNode<?>> createMultipleInputJoinNodes(
            ReadableConfig tableConfig, List<ExecNodeWrapper> rootWrappers) {
        List<ExecNode<?>> result = new ArrayList<>();
        Map<ExecNodeWrapper, ExecNode<?>> visitedMap = new HashMap<>();
        for (ExecNodeWrapper rootWrapper : rootWrappers) {
            result.add(getMultipleInputJoinNode(tableConfig, rootWrapper, visitedMap));
        }
        return result;
    }

    private ExecNode<?> getMultipleInputJoinNode(
            ReadableConfig tableConfig,
            ExecNodeWrapper wrapper,
            Map<ExecNodeWrapper, ExecNode<?>> visitedMap) {
        if (visitedMap.containsKey(wrapper)) {
            return visitedMap.get(wrapper);
        }

        for (int i = 0; i < wrapper.inputs.size(); i++) {
            ExecNode<?> MultipleInputJoinNode =
                    getMultipleInputJoinNode(tableConfig, wrapper.inputs.get(i), visitedMap);
            ExecEdge execEdge =
                    ExecEdge.builder()
                            .source(MultipleInputJoinNode)
                            .target(wrapper.execNode)
                            .build();
            wrapper.execNode.replaceInputEdge(i, execEdge);
        }

        ExecNode<?> ret;
        if (wrapper.group != null && wrapper == wrapper.group.root) {
            ret = createMultipleInputJoinNode(tableConfig, wrapper.group, visitedMap);
        } else {
            ret = wrapper.execNode;
        }
        visitedMap.put(wrapper, ret);
        return ret;
    }

    private ExecNode<?> createMultipleInputJoinNode(
            ReadableConfig tableConfig,
            MultipleInputJoinGroup group,
            Map<ExecNodeWrapper, ExecNode<?>> visitedMap) {
        // calculate the inputs of the multiple input node
        List<Tuple3<ExecNode<?>, InputProperty, ExecEdge>> inputs = new ArrayList<>();
        for (ExecNodeWrapper member : group.members) {
            for (int i = 0; i < member.inputs.size(); i++) {
                ExecNodeWrapper memberInput = member.inputs.get(i);
                if (group.members.contains(memberInput)) {
                    continue;
                }
                Preconditions.checkState(
                        visitedMap.containsKey(memberInput),
                        "Input of a multiple input member is not visited. This is a bug.");

                ExecNode<?> inputNode = visitedMap.get(memberInput);
                InputProperty inputProperty = member.execNode.getInputProperties().get(i);
                ExecEdge edge = member.execNode.getInputEdges().get(i);
                inputs.add(Tuple3.of(inputNode, inputProperty, edge));
            }
        }
        // return StreamMultipleInputJoinNode
        return (ExecNode<?>) createStreamMultipleInputJoinNode(tableConfig, group, inputs);
    }

    private StreamExecKeyedBroadcastMultipleInputJoin<RowData> createStreamMultipleInputJoinNode(
            ReadableConfig tableConfig,
            MultipleInputJoinGroup group,
            List<Tuple3<ExecNode<?>, InputProperty, ExecEdge>> inputs) {
        ExecNode<?> rootNode = group.root.execNode;
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        List<InputProperty> inputProperties = new ArrayList<>();
        List<ExecEdge> originalEdges = new ArrayList<>();
        for (Tuple3<ExecNode<?>, InputProperty, ExecEdge> tuple3 : inputs) {
            ExecNode<?> inputNode = tuple3.f0;
            inputNodes.add(inputNode);
            ExecEdge edge = tuple3.f2;
            originalEdges.add(edge);
            InputProperty originalInputEdge = tuple3.f1;
            inputProperties.add(
                    InputProperty.builder()
                            .requiredDistribution(originalInputEdge.getRequiredDistribution())
                            .damBehavior(originalInputEdge.getDamBehavior())
                            .build());
        }
        String description =
                ExecNodeUtil.getMultipleInputJoinDescription(rootNode, inputNodes, inputProperties);
        StreamExecKeyedBroadcastMultipleInputJoin<RowData> multipleInputJoin;
        try {
            multipleInputJoin =
                    new StreamExecKeyedBroadcastMultipleInputJoin<RowData>(
                            tableConfig, inputProperties, rootNode, originalEdges, description);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<ExecEdge> inputEdges = new ArrayList<>(inputNodes.size());
        for (ExecNode<?> inputNode : inputNodes) {
            inputEdges.add(ExecEdge.builder().source(inputNode).target(multipleInputJoin).build());
        }
        multipleInputJoin.setInputEdges(inputEdges);
        return multipleInputJoin;
    }
    // --------------------------------------------------------------------------------
    // Helper Classes
    // --------------------------------------------------------------------------------

    private static class ExecNodeWrapper {
        private final ExecNode<?> execNode;
        private final List<ExecNodeWrapper> inputs;
        private final List<ExecNodeWrapper> outputs;
        private MultipleInputJoinGroup group;

        private ExecNodeWrapper(ExecNode<?> execNode) {
            this.execNode = execNode;
            this.inputs = new ArrayList<>();
            this.outputs = new ArrayList<>();
            this.group = null;
        }
    }

    private static class MultipleInputJoinGroup {
        // members in group
        private final List<ExecNodeWrapper> members;
        // root of group
        private ExecNodeWrapper root;

        private Integer joinCount = 0;

        private MultipleInputJoinGroup(ExecNodeWrapper root) {
            this.members = new ArrayList<>();
            members.add(root);
            this.root = root;
            if (root.execNode instanceof StreamExecJoin) {
                joinCount++;
            }
        }

        public Integer getJoinCount() {
            return joinCount;
        }

        private void addMember(ExecNodeWrapper wrapper) {
            Preconditions.checkState(
                    wrapper.group == null,
                    "The given exec node wrapper is already in a multiple input join group. This is a bug.");
            members.add(wrapper);
            wrapper.group = this;
            if (wrapper.execNode instanceof StreamExecJoin) {
                joinCount++;
            }
        }

        private void removeMember(ExecNodeWrapper wrapper) {
            Preconditions.checkState(
                    members.remove(wrapper),
                    "The given exec node wrapper does not exist in the multiple input join group. This is a bug.");
            wrapper.group = null;
            if (wrapper.execNode instanceof StreamExecJoin) {
                joinCount--;
            }
        }
    }
}
