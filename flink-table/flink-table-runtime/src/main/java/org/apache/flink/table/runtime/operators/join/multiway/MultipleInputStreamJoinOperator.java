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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.MultipleInputJoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.AbstractMultipleInputJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.MultipleInputJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * The multi-join operator,only support inner join and accumulate records now.
 *
 * @author Quentin Qiu
 */
public class MultipleInputStreamJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {
    private final int numberOfInputs;
    private transient List<AbstractMultipleInputJoinRecordStateView> recordStateViews;
    private transient List<List<Integer>> joinOrders;
    private final List<JoinInputSideSpec> inputSideSpecs;
    private final List<InternalTypeInfo<RowData>> internalTypeInfos;
    private final long stateRetentionTime;
    protected transient TimestampedCollector<RowData> collector;
    private final List<List<GeneratedJoinCondition>> generatedJoinConditionsList;
    private transient List<List<JoinConditionWithNullFilters>> joinConditionLists;
    private Selectivity selectivity;

    private static final Long DELAY = (long) 60;
    private static final Long PERIOD = (long) 60;

    final Logger logger = LoggerFactory.getLogger(MultipleInputStreamJoinOperator.class);
    boolean adaptive = true;

    public MultipleInputStreamJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            int numberOfInputs,
            List<JoinInputSideSpec> inputSideSpecs,
            List<InternalTypeInfo<RowData>> internalTypeInfos,
            List<List<GeneratedJoinCondition>> generatedJoinConditionsList,
            long stateRetentionTime) {
        super(parameters, numberOfInputs);
        this.numberOfInputs = numberOfInputs;
        this.inputSideSpecs = inputSideSpecs;
        this.internalTypeInfos = internalTypeInfos;
        this.generatedJoinConditionsList = generatedJoinConditionsList;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open() throws Exception {
        super.open();
        selectivity = new Selectivity();
        updateJoinOrders();
        this.joinConditionLists = getConditions();
        initStates();
        this.collector = new TimestampedCollector<>(output);
        //ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Map<String, String> globConf = globalJobParameters.toMap();
        if (globConf.containsKey("adaptive")){
            if(globConf.get("adaptive").equals("false")){
                adaptive = false;
                logger.info("MJ operator's adaptive closed!");
            }
        }
        if(adaptive){
            setUpdateScheduledExecutor();
        }
        logger.info("open");
    }

    private void setUpdateScheduledExecutor() {
        Runnable runable =
                new Runnable() {
                    public void run() {
                        selectivity.updateSelectivity();
                        updateJoinOrders();
                        printUpdateInfos();
                        selectivity.resetCount();
                        printUpdateInfos();
                    }
                };
        ScheduledExecutorService service =
                new ScheduledThreadPoolExecutor(
                        1,
                        new BasicThreadFactory.Builder()
                                .namingPattern("update-schedule-pool-%d")
                                .daemon(true)
                                .build());
        service.scheduleAtFixedRate(runable, DELAY, PERIOD, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // close condition
        for (List<JoinConditionWithNullFilters> joinConditions : joinConditionLists) {
            for (JoinConditionWithNullFilters joinCondition : joinConditions) {
                joinCondition.close();
            }
        }
    }

    /** get List<MultipleInputJoinInput(operator:this,inputIndex)> */
    @Override
    public List<Input> getInputs() {
        ArrayList<Input> inputs = new ArrayList<>();
        for (int inputId = 1; inputId < numberOfInputs + 1; inputId++) {
            MultipleInputJoinInput input = new MultipleInputJoinInput(this, inputId);
            inputs.add(input);
        }
        return inputs;
    }

    /**
     * Process an input element and output incremental joined recprds. Outer join is not supported
     * now. Restract records are not supported now.
     */
    public void processElementByIndex(StreamRecord<RowData> element, Integer inputIndex)
            throws Exception {
        boolean doCollect = true;
        RowData input = element.getValue();
        // add record to state
        recordStateViews.get(inputIndex).addRecord(input);
        RowKind inputRowKind = input.getRowKind();
        // only support insert
        if (inputRowKind != RowKind.INSERT) {
            throw new RuntimeException(
                    "MultipleInputStreamJoinOperator only support insert record");
        }
        // get joinOrder according to index，and execute join operation according to joinOrder
        List<Integer> joinOrder = joinOrders.get(inputIndex);
        List<List<RowData>> associatedRowsList =
                new ArrayList<>(Collections.nCopies(numberOfInputs, null));
        // add input to a new list
        List<RowData> inputList = new ArrayList<>();
        inputList.add(input);
        // associatedRowsList,add inputList to the right position
        associatedRowsList.set(inputIndex, inputList);
        // get associated rows list,every associated rows correspond to matched rows in every state
        for (int joinIndex : joinOrder) {
            if (joinIndex != inputIndex) {
                List<RowData> associatedRows;
                if (inputIndex < joinIndex) {
                    associatedRows =
                            matchRows(
                                    input,
                                    false,
                                    recordStateViews.get(joinIndex),
                                    joinConditionLists.get(joinIndex).get(inputIndex));
                } else {
                    associatedRows =
                            matchRows(
                                    input,
                                    true,
                                    recordStateViews.get(joinIndex),
                                    joinConditionLists.get(inputIndex).get(joinIndex));
                }
                selectivity.matchCount[inputIndex][joinIndex] += associatedRows.size();
                selectivity.rowsCount[inputIndex][joinIndex] += recordStateViews.get(joinIndex).getRecordSize();
                if (associatedRows.isEmpty()) {
                    doCollect = false;
                    break;
                }
                associatedRowsList.set(joinIndex, associatedRows);
            }
        }
        // if every associatedRows is not empty
        if (doCollect) {
            // using cross join to convert associated rows list
            List<List<RowData>> joinedRows = crossJoin(associatedRowsList);
            for (List<RowData> joinedRow : joinedRows) {
                collector.collect(new MultipleInputJoinedRowData(RowKind.INSERT, joinedRow));
            }
        }
    }
    /** initialize the join order according the input order */
    private List<List<Integer>> getInitOrders() {
        int numberOfInputs = this.numberOfInputs;
        List<List<Integer>> orders = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            List<Integer> order = new ArrayList<>();
            for (int j = 0; j < numberOfInputs; j++) {
                if (j != i) {
                    order.add(j);
                }
            }
            orders.add(order);
        }
        return orders;
    }

    /** init states */
    private void initStates() {
        recordStateViews = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            String stateName = "input" + i;
            AbstractMultipleInputJoinRecordStateView joinRecordStateView =
                    MultipleInputJoinRecordStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSideSpecs.get(i),
                            internalTypeInfos.get(i),
                            stateRetentionTime);
            recordStateViews.add(joinRecordStateView);
        }
    }

    /** get JoinConditionWithNullFilters */
    private List<List<JoinConditionWithNullFilters>> getConditions() throws Exception {
        List<List<JoinConditionWithNullFilters>> joinConditionWithNullFiltersList =
                new ArrayList<>();
        for (List<GeneratedJoinCondition> generatedJoinConditions : generatedJoinConditionsList) {
            List<JoinConditionWithNullFilters> joinConditionWithNullFilters = new ArrayList<>();
            for (GeneratedJoinCondition generatedJoinCondition : generatedJoinConditions) {
                JoinCondition condition =
                        generatedJoinCondition.newInstance(
                                getRuntimeContext().getUserCodeClassLoader());
                JoinConditionWithNullFilters joinCondition =
                        new JoinConditionWithNullFilters(condition, new boolean[] {true}, this);
                joinCondition.setRuntimeContext(getRuntimeContext());
                joinCondition.open(new Configuration());
                joinConditionWithNullFilters.add(joinCondition);
            }
            joinConditionWithNullFiltersList.add(joinConditionWithNullFilters);
        }
        return joinConditionWithNullFiltersList;
    }
    /** input get matched rows from otherSideStateView using joinCondition */
    public static List<RowData> matchRows(
            RowData input,
            boolean inputIsLeft,
            AbstractMultipleInputJoinRecordStateView otherSideStateView,
            JoinCondition condition)
            throws Exception {
        List<RowData> associations = new ArrayList<>();
        Iterable<RowData> records = otherSideStateView.getRecords();
        for (RowData record : records) {
            boolean matched =
                    inputIsLeft ? condition.apply(input, record) : condition.apply(record, input);
            if (matched) {
                // use -1 as the default number of associations
                associations.add(record);
            }
        }
        return associations;
    }

    /** cross join({{a1},{b1,b2},{c1}})-> {a1,b1,c1},{a1,b2,c1}} */
    public static List<List<RowData>> crossJoin(List<List<RowData>> lists) {
        // 如果输入的列表为空，则返回空列表
        if (lists == null || lists.isEmpty()) {
            return new ArrayList<>();
        }
        // 如果输入的列表只有一个，则将其每个元素放入一个列表中，作为结果返回
        if (lists.size() == 1) {
            List<List<RowData>> result = new ArrayList<>();
            for (RowData row : lists.get(0)) {
                List<RowData> rowList = new ArrayList<>();
                rowList.add(row);
                result.add(rowList);
            }
            return result;
        }
        // 取出第一个列表，并对其余列表进行递归调用
        List<RowData> firstList = lists.get(0);
        List<List<RowData>> subLists = crossJoin(lists.subList(1, lists.size()));
        // 将第一个列表中的每个元素依次与递归调用得到的列表中的元素进行组合
        List<List<RowData>> result = new ArrayList<>();
        for (RowData row : firstList) {
            for (List<RowData> subList : subLists) {
                List<RowData> newList = new ArrayList<>();
                newList.add(row);
                newList.addAll(subList);
                result.add(newList);
            }
        }
        return result;
    }

    private void updateJoinOrders() {
        List<List<Integer>> joinOrders = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            List<Double> selectivities = selectivity.selectivities.get(i);
            List<Integer> order = new ArrayList<>();
            // sort original list and get correspond index
            int[] sortedIndexArr =
                    IntStream.range(0, selectivities.size())
                            .boxed()
                            .sorted(Comparator.comparingDouble(selectivities::get))
                            .mapToInt(Integer::intValue)
                            .toArray();
            for (int index : sortedIndexArr) {
                order.add(index);
            }
            joinOrders.add(order);
        }
        this.joinOrders = joinOrders;
    }

    private void printUpdateInfos() {
        logger.info(String.format("new rowscount：%s", Arrays.deepToString(selectivity.rowsCount)));
        logger.info(
                String.format("new matchcount：%s", Arrays.deepToString(selectivity.matchCount)));
        logger.info("new selectivities：" + selectivity.selectivities);
        logger.info("new join order：" + joinOrders);
    }

    private class Selectivity {
        // matchCount[i][j] means the count of matched rows during ith input visiting the jth input
        public int[][] matchCount = new int[numberOfInputs][numberOfInputs];
        // rowsCount[i][j] means the sum of jth input count during visiting
        public Long[][] rowsCount = new Long[numberOfInputs][numberOfInputs];
        // selectivity[i][j] means the selectivity
        public List<List<Double>> selectivities;

        public Selectivity() {
            initSelectivity();
        }

        private void initSelectivity() {
            List<List<Double>> selectivities = new ArrayList<>();
            for (int i = 0; i < numberOfInputs; i++) {
                List<Double> selectivity = new ArrayList<>();
                for (int j = 0; j < numberOfInputs; j++) {
                    selectivity.add(1.0);
                    this.rowsCount[i][j] = 0L;
                }
                selectivities.add(selectivity);
            }
            this.selectivities = selectivities;
        }

        public void updateSelectivity() {
            for (int i = 0; i < numberOfInputs; i++) {
                for (int j = 0; j < numberOfInputs; j++) {
                    if (i == j) {
                        continue;
                    }
                    if (rowsCount[i][j] != 0) {
                        selectivities.get(i).set(j, (double)matchCount[i][j] / (double)rowsCount[i][j]);
                    }
                }
            }
        }

        public void resetCount() {
            for (int i = 0; i < numberOfInputs; i++) {
                for (int j = 0; j < numberOfInputs; j++) {
                    this.rowsCount[i][j] = 0L;
                    this.matchCount[i][j] = 0;
                }
            }
            logger.info("reset counts");
        }
    }
}
