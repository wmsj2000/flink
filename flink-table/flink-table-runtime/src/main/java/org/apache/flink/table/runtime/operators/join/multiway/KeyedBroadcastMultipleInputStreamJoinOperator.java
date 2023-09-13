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

package org.apache.flink.table.runtime.operators.join.multiway;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.MultipleInputJoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.AbstractKeyedBroadcastMultipleInputJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.KeyedBroadcastMultipleInputJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.MultipleInputJoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The multi-join operator,only support inner join and accumulate records now.
 *
 * @author Quentin Qiu
 */
public class KeyedBroadcastMultipleInputStreamJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {
    private final int numberOfInputs;
    private transient List<AbstractKeyedBroadcastMultipleInputJoinRecordStateView> recordStateViews;
    private final List<MultipleInputJoinInputSideSpec> inputSideSpecs;
    private final List<InternalTypeInfo<RowData>> internalTypeInfos;
    private final long stateRetentionTime;
    protected transient TimestampedCollector<RowData> collector;
    private final MultipleInputJoinEdge[][] multipleInputJoinEdges;
    private transient JoinConditionWithNullFilters[][] joinConditions;
    private transient List<List<MultipleInputJoinEdge>> joinPaths;

    final Logger logger = LoggerFactory.getLogger(MultipleInputStreamJoinOperator.class);

    public KeyedBroadcastMultipleInputStreamJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            int numberOfInputs,
            List<MultipleInputJoinInputSideSpec> inputSideSpecs,
            List<InternalTypeInfo<RowData>> internalTypeInfos,
            MultipleInputJoinEdge[][] multipleInputJoinEdges,
            long stateRetentionTime) {
        super(parameters, numberOfInputs);
        this.numberOfInputs = numberOfInputs;
        this.inputSideSpecs = inputSideSpecs;
        this.internalTypeInfos = internalTypeInfos;
        this.stateRetentionTime = stateRetentionTime;
        this.multipleInputJoinEdges = multipleInputJoinEdges;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.recordStateViews = initStates();
        this.joinConditions = getConditions();
        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // close conditions
        for (int i = 0; i < numberOfInputs; i++) {
            for (int j = 0; j < numberOfInputs; j++) {
                if (joinConditions[i][j] != null) {
                    joinConditions[i][j].close();
                }
            }
        }
    }

    /** get List<KeyedBroadcastMultipleInputJoinInput(operator:this,inputIndex)> */
    @Override
    public List<Input> getInputs() {
        ArrayList<Input> inputs = new ArrayList<>();
        for (int inputId = 1; inputId < numberOfInputs + 1; inputId++) {
            KeyedBroadcastMultipleInputJoinInput input =
                    new KeyedBroadcastMultipleInputJoinInput(this, inputId);
            inputs.add(input);
        }
        return inputs;
    }

    public void processElementByIndex(StreamRecord<RowData> element, Integer inputIndex)
            throws Exception {
        RowData input = element.getValue();
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        // erase RowKind for later state updating
        input.setRowKind(RowKind.INSERT);
        if (isAccumulateMsg) {
            recordStateViews.get(inputIndex).addRecord(input);
        } else {
            recordStateViews.get(inputIndex).retractRecord(input);
        }
        List<RowData> inputList = new ArrayList<>();
        inputList.add(input);
        boolean[] visited = new boolean[numberOfInputs];
        List<Integer> path = new ArrayList<>();
        List<List<RowData>> associatedLists =
                dfsJoin2(multipleInputJoinEdges, inputList, inputIndex, visited);
        for (List<RowData> associated : associatedLists) {
            if (!associated.contains(null)) {
                List<RowData> projectedAssociated = projectAssociated(associated, inputSideSpecs);
                MultipleInputJoinedRowData multipleInputJoinedRowData =
                        new MultipleInputJoinedRowData(inputRowKind, projectedAssociated);
                collector.collect(multipleInputJoinedRowData);
            }
        }
    }

    private List<RowData> projectAssociated(
            List<RowData> associated, List<MultipleInputJoinInputSideSpec> inputSideSpecs)
            throws Exception {
        List<RowData> projectedAssociated = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            RowData projectedData =
                    inputSideSpecs.get(i).getOutputSelector().getKey(associated.get(i));
            projectedAssociated.add(projectedData);
        }
        return projectedAssociated;
    }

    private List<List<RowData>> dfsJoin2(
            MultipleInputJoinEdge[][] multipleInputJoinEdges,
            List<RowData> inputs,
            int inputIndex,
            boolean[] visited)
            throws Exception {
        List<List<RowData>> lists = new ArrayList<>();
        for (RowData input : inputs) {
            boolean[] visitedCopy = Arrays.copyOfRange(visited,0, numberOfInputs);
            List<RowData> leftRecords = new ArrayList<>();
            leftRecords.add(input);
            List<List<RowData>> list = new ArrayList<>();
            List<RowData> current = new ArrayList<>(Collections.nCopies(numberOfInputs, null));
            current.set(inputIndex, input);
            list.add(current);
            for (int i = 0; i < numberOfInputs; i++) {
                if (multipleInputJoinEdges[inputIndex][i] != null && !visited[i]) {
                    visited[i] = true;
                    JoinCondition condition = joinConditions[inputIndex][i];
                    AbstractKeyedBroadcastMultipleInputJoinRecordStateView rightState =
                            recordStateViews.get(i);
                    List<RowData> associatedRecords =
                            matchRows(leftRecords, inputIndex, rightState, condition);
                    if (associatedRecords.isEmpty()) {
                        list = null;
                        break;
                    }
                    List<List<RowData>> dfs =
                            dfsJoin2(multipleInputJoinEdges, associatedRecords, i, visited);
                    list = combineAssociatedLists(list, dfs);
                }
            }
            if (list != null) {
                lists.addAll(list);
            }
            visited = visitedCopy;
        }
        return lists;
    }

    private List<List<RowData>> combineAssociatedLists(
            List<List<RowData>> combinedLists, List<List<RowData>> mergedLists) {
        List<List<RowData>> combineAssociatedLists = new ArrayList<>();
        if (combinedLists == null || mergedLists == null) {
            return null;
        }
        for (List<RowData> list1 : combinedLists) {
            for (List<RowData> list2 : mergedLists) {
                combineAssociatedLists.add(mergeList(list1, list2));
            }
        }
        return combineAssociatedLists;
    }

    private List<RowData> mergeList(List<RowData> list1, List<RowData> list2) {
        List<RowData> mergedList = new ArrayList<>(Collections.nCopies(list1.size(), null));
        for (int i = 0; i < list1.size(); i++) {
            RowData data1 = list1.get(i);
            RowData data2 = list2.get(i);
            if (data1 == null && data2 != null) {
                mergedList.set(i, data2);
            } else if (data1 != null && data2 == null) {
                mergedList.set(i, data1);
            } else if (data1 == null && data2 == null) {
                mergedList.set(i, null);
            } else {
                throw new RuntimeException("mergeList error");
            }
        }
        return mergedList;
    }

    /** input get matched rows from otherSideStateView using joinCondition */
    public List<RowData> matchRows(
            List<RowData> leftRecords,
            int leftIndex,
            AbstractKeyedBroadcastMultipleInputJoinRecordStateView otherSideStateView,
            JoinCondition condition)
            throws Exception {
        List<RowData> associations = new ArrayList<>();
        for (RowData leftRecord : leftRecords) {
            Iterable<RowData> records = otherSideStateView.getRecords(leftRecord, leftIndex);
            for (RowData rightRecord : records) {
                boolean matched = condition.apply(leftRecord, rightRecord);
                if (matched) {
                    associations.add(rightRecord);
                }
            }
        }
        return associations;
    }

    private JoinConditionWithNullFilters[][] getConditions() throws Exception {
        JoinConditionWithNullFilters[][] joinConditions =
                new JoinConditionWithNullFilters[numberOfInputs][numberOfInputs];
        for (int i = 0; i < numberOfInputs; i++) {
            for (int j = 0; j < numberOfInputs; j++) {
                if (multipleInputJoinEdges[i][j] == null) {
                    continue;
                }
                GeneratedJoinCondition generatedJoinCondition =
                        multipleInputJoinEdges[i][j].getGeneratedJoinCondition();
                JoinCondition condition =
                        generatedJoinCondition.newInstance(
                                getRuntimeContext().getUserCodeClassLoader());
                JoinConditionWithNullFilters joinCondition =
                        new JoinConditionWithNullFilters(condition, new boolean[] {}, this);
                joinCondition.setRuntimeContext(getRuntimeContext());
                joinCondition.open(new Configuration());
                joinConditions[i][j] = joinCondition;
            }
        }
        return joinConditions;
    }

    private List<AbstractKeyedBroadcastMultipleInputJoinRecordStateView> initStates()
            throws Exception {
        List<AbstractKeyedBroadcastMultipleInputJoinRecordStateView> recordStateViews =
                new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            String stateName = "input" + i;
            AbstractKeyedBroadcastMultipleInputJoinRecordStateView joinRecordStateView =
                    KeyedBroadcastMultipleInputJoinRecordStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSideSpecs.get(i),
                            numberOfInputs,
                            this,
                            internalTypeInfos.get(i),
                            multipleInputJoinEdges,
                            stateRetentionTime);
            recordStateViews.add(joinRecordStateView);
        }
        return recordStateViews;
    }
}
