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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private static Long DELAY = (long) 30;
    private static Long PERIOD = (long) 60;
    private static Long COLLECTTIME = (long) 3;
    private static boolean isAdaptive = false;
    private boolean isCollecting = false;
    private boolean resetCount = false;
    private double QUERY_COST_COFFICIENT = 5.0;
    private double MATCH_COST_COFFICIENT = 1.0;
    private long queryTime = 0;
    private long queryCount = 0;
    private long matchTime = 0;
    private long matchCount = 0;
    private String stateBackend = "fileSystem";


    final Logger logger = LoggerFactory.getLogger(MultipleInputStreamJoinOperator.class);

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
        initOrders();
        initStates();
        getParameters();
        this.selectivity = new Selectivity();
        this.joinConditionLists = getConditions();
        this.collector = new TimestampedCollector<>(output);
        // set update scheduled executor
        if (isAdaptive) {
            setUpdateScheduledExecutor();
        } else {
            logger.info("\nMJ operator's adaptive closed!");
        }
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

    private void getParameters() {
        ExecutionConfig.GlobalJobParameters globalJobParameters =
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Map<String, String> globConf = globalJobParameters.toMap();
        try {
            String adaptive = globConf.getOrDefault("adaptive", "false");
            isAdaptive = Boolean.parseBoolean(adaptive);
            String period = globConf.getOrDefault("period", "60");
            PERIOD = Long.parseLong(period);
            String delay = globConf.getOrDefault("delay", "30");
            DELAY = Long.parseLong(delay);
            String collect = globConf.getOrDefault("collect", "3");
            COLLECTTIME = Long.parseLong(collect);
            String reset = globConf.getOrDefault("reset", "false");
            resetCount = Boolean.parseBoolean(reset);
            String queryCost = globConf.getOrDefault("query_cost", "1");
            QUERY_COST_COFFICIENT = Double.parseDouble(queryCost);
            String matchCost = globConf.getOrDefault("match_cost", "10");
            MATCH_COST_COFFICIENT = Double.parseDouble(matchCost);
            stateBackend = globConf.getOrDefault("stateBackend", "fileSystem");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setUpdateScheduledExecutor() {
        Runnable task1 =
                new Runnable() {
                    public void run() {
                        isCollecting = true;
                        SimpleDateFormat formatter =
                                new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                        Date date = new Date(System.currentTimeMillis());
                        logger.info("collect true：" + formatter.format(date));
                    }
                };
        Runnable task2 =
                new Runnable() {
                    public void run() {
                        long startTime = System.currentTimeMillis();
                        selectivity.updateSelectivity();
                        updateJoinOrdersByMatchRate();
                        printUpdateInfos();
                        if (resetCount) {
                            selectivity.resetCount();
                        }
                        isCollecting = false;
                        SimpleDateFormat formatter =
                                new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                        Date date = new Date(System.currentTimeMillis());
                        long endTime = System.currentTimeMillis();
                        logger.info("update time:" + (endTime-startTime));
                        logger.info("collect false：" + formatter.format(date));
                    }
                };
        ScheduledExecutorService service =
                new ScheduledThreadPoolExecutor(
                        1,
                        new BasicThreadFactory.Builder()
                                .namingPattern("update-schedule-pool-%d")
                                .daemon(true)
                                .build());
        logger.info(
                "\nMJ operator's adaptive open!"
                        + "\n"
                        + "delay:"
                        + DELAY
                        + "s"
                        + "\n"
                        + "period:"
                        + PERIOD
                        + "s"
                        + "\n"
                        + "collect time:"
                        + COLLECTTIME
                        + "s");
        service.scheduleAtFixedRate(task1, DELAY, PERIOD, TimeUnit.SECONDS);
        service.scheduleAtFixedRate(task2, DELAY + COLLECTTIME, PERIOD, TimeUnit.SECONDS);
    }

    private void printUpdateInfos() {
        logger.info(
                        "\n"
                        + "new match rate："
                        + Arrays.deepToString(selectivity.matchRates)
                        + "\n"
                        + "new avgOfBeMatched："
                        + Arrays.deepToString(selectivity.avgOfBeMatched)
                        + "\n"
                        + "new join order："
                        + joinOrders
                        + "\n"
                        + "match time avg："
                        + matchTime/matchCount
                        + "\n"
                        + "query time avg："
                        + queryTime/queryCount
                        +"\n"
                        + "query cost："
                        + Arrays.deepToString(selectivity.queryCost)
        );
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
        long startTime0 = System.nanoTime();
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
                long numberOfAssociated = associatedRows.size();
                if (numberOfAssociated > 0) {
                    selectivity.countOfBeMatched[inputIndex][joinIndex] += 1;
                    selectivity.sumOfBeMatched[inputIndex][joinIndex] += numberOfAssociated;
                }
                if (isCollecting & isAdaptive) {
                    selectivity.matchCount[inputIndex][joinIndex] += numberOfAssociated > 0 ? 1 : 0;
                    selectivity.probeCount[inputIndex][joinIndex] += 1;
                }
                if (associatedRows.isEmpty()) {
                    doCollect = false;
                    if (!isCollecting) {
                        break;
                    }
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
    public List<RowData> matchRows(
            RowData input,
            boolean inputIsLeft,
            AbstractMultipleInputJoinRecordStateView otherSideStateView,
            JoinCondition condition)
            throws Exception {
        List<RowData> associations = new ArrayList<>();
        long s1 = System.nanoTime();
        Iterable<RowData> records = otherSideStateView.getRecords();
        long e1 = System.nanoTime();
        queryCount++;
        queryTime+=(e1-s1);
        for (RowData record : records) {
            long s2 = System.nanoTime();
            boolean matched =
                    inputIsLeft ? condition.apply(input, record) : condition.apply(record, input);
            if (matched) {
                // use -1 as the default number of associations
                associations.add(record);
            }
            long e2 = System.nanoTime();
            matchCount++;
            matchTime+=(e2-s2);
        }
        return associations;
    }

    /** cross join({{a1},{b1,b2},{c1}})-> {a1,b1,c1},{a1,b2,c1}} */
    public List<List<RowData>> crossJoin(List<List<RowData>> lists) {
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

    /** initialize the join order according the input order */
    private void initOrders() {
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
        this.joinOrders = orders;
    }

    private void updateJoinOrdersByMatchRate() {
        List<List<Integer>> initOrders = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            List<Integer> order = new ArrayList<>();
            for (int j = 0; j < numberOfInputs; j++) {
                if (j != i) {
                    order.add(j);
                }
            }
            initOrders.add(order);
        }
        List<List<Integer>> joinOrders = new ArrayList<>();
        for (int i = 0; i < numberOfInputs; i++) {
            List<Integer> joinOrder = initOrders.get(i);
            List<List<Integer>> allOrders = permute(initOrders.get(i));
            double minCost = Integer.MAX_VALUE;
            for (List<Integer> order : allOrders) {
                double cost =
                        calculateTotalCost(
                                selectivity.matchRates[i], selectivity.queryCost[i],selectivity.avgOfBeMatched[i], order);
                if (cost < minCost) {
                    minCost = cost;
                    joinOrder = order;
                }
            }
            joinOrders.add(joinOrder);
        }
        this.joinOrders = joinOrders;
    }

    public double calculateTotalCost(double[] rate, double[] queryCost, double[] matchCost, List<Integer> indexOrder) {
        return calculateTotalCostHelper(rate, queryCost, matchCost, indexOrder, 0);
    }

    private double calculateTotalCostHelper(
            double[] rate, double[] queryCost, double[] matchCost, List<Integer> indexOrder, int index) {
        if (index == indexOrder.size()) {
            return 0;
        }
        int current = indexOrder.get(index);
        double subCost = calculateTotalCostHelper(rate, queryCost, matchCost, indexOrder, index + 1);
        return QUERY_COST_COFFICIENT * queryCost[current] + rate[current] * (MATCH_COST_COFFICIENT * matchCost[current] + subCost);
    }

    public static List<List<Integer>> permute(List<Integer> nums) {
        List<List<Integer>> res = new ArrayList<>();
        if (nums.size() == 1) {
            List<Integer> list = new ArrayList<>();
            list.add(nums.get(0));
            res.add(list);
            return res;
        }
        for (int i = 0; i < nums.size(); i++) {
            List<Integer> rest = getRest(nums, i);
            List<List<Integer>> restPermutes = permute(rest);
            for (List<Integer> permute : restPermutes) {
                List<Integer> list = new ArrayList<>();
                list.add(nums.get(i));
                list.addAll(permute);
                res.add(list);
            }
        }
        return res;
    }

    private static List<Integer> getRest(List<Integer> nums, int index) {
        List<Integer> rest = new ArrayList<>();
        for (int i = 0; i < nums.size(); i++) {
            if (i != index) {
                rest.add(nums.get(i));
            }
        }
        return rest;
    }

    private class Selectivity {
        public long[][] matchCount = new long[numberOfInputs][numberOfInputs];
        public long[][] probeCount = new long[numberOfInputs][numberOfInputs];
        public double[][] matchRates = new double[numberOfInputs][numberOfInputs];
        public long[][] sumOfBeMatched = new long[numberOfInputs][numberOfInputs];
        public long[][] countOfBeMatched = new long[numberOfInputs][numberOfInputs];
        public double[][] avgOfBeMatched = new double[numberOfInputs][numberOfInputs];
        public double[][] queryCost = new double[numberOfInputs][numberOfInputs];

        public Selectivity() {
            initSelectivity();
        }

        private void initSelectivity() {
            for (int i = 0; i < numberOfInputs; i++) {
                for (int j = 0; j < numberOfInputs; j++) {
                    matchRates[i][j] = 1.0;
                    queryCost[i][j] = 1.0;
                }
            }
        }

        public void updateSelectivity() {
            for (int i = 0; i < numberOfInputs; i++) {
                for (int j = 0; j < numberOfInputs; j++) {
                    if (i == j) {
                        continue;
                    }
                    if (probeCount[i][j] != 0 && countOfBeMatched[i][j] != 0) {
                        matchRates[i][j] = (double) matchCount[i][j] / (double) probeCount[i][j];
                        avgOfBeMatched[i][j] =
                                (double) sumOfBeMatched[i][j] / (double) countOfBeMatched[i][j];
                    }

                }
            }
            if(stateBackend.equals("rocksdb")){
                for (int i = 0; i < numberOfInputs; i++) {
                    for (int j = 0; j < numberOfInputs; j++) {
                        double queryCostRocksdb  = Math.log(recordStateViews.get(j).getRecordSize())/ Math.log(2);
                        queryCost[i][j] = queryCostRocksdb;
                    }
                }
            }
        }

        public void resetCount() {
            for (int i = 0; i < numberOfInputs; i++) {
                for (int j = 0; j < numberOfInputs; j++) {
                    this.matchCount[i][j] = 0L;
                    this.probeCount[i][j] = 0L;
                }
            }
            logger.info("\nreset counts");
        }
    }
}
