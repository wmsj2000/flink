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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.multiway.KeyedBroadcastMultipleInputStreamJoinOperator;
import org.apache.flink.table.runtime.operators.join.multiway.MultipleInputJoinEdge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility to create a {@link AbstractMultipleInputJoinRecordStateView} depends on {@link
 * JoinInputSideSpec}.
 *
 * @author Quentin Qiu
 */
public final class KeyedBroadcastMultipleInputJoinRecordStateViews {
    public static AbstractKeyedBroadcastMultipleInputJoinRecordStateView create(
            RuntimeContext ctx,
            String stateName,
            MultipleInputJoinInputSideSpec inputSideSpec,
            int numberOfInputs,
            KeyedBroadcastMultipleInputStreamJoinOperator operator,
            InternalTypeInfo<RowData> recordType,
            MultipleInputJoinEdge[][] multipleInputJoinEdges,
            long retentionTime)
            throws Exception {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.isBroadcast()) {
            return createBroadcastState(
                    ctx,
                    stateName,
                    inputSideSpec,
                    numberOfInputs,
                    operator,
                    multipleInputJoinEdges,
                    ttlConfig);
        } else {
            return createKeyedState(
                    ctx,
                    stateName,
                    inputSideSpec,
                    numberOfInputs,
                    operator,
                    recordType,
                    multipleInputJoinEdges,
                    retentionTime);
        }
    }

    public static AbstractKeyedBroadcastMultipleInputJoinRecordStateView createKeyedState(
            RuntimeContext ctx,
            String stateName,
            MultipleInputJoinInputSideSpec inputSideSpec,
            int numberOfInputs,
            KeyedBroadcastMultipleInputStreamJoinOperator operator,
            InternalTypeInfo<RowData> recordType,
            MultipleInputJoinEdge[][] multipleInputJoinEdges,
            long retentionTime)
            throws Exception {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new MultipleInputJoinKeyContainsUniqueKey(
                        ctx, stateName, recordType, ttlConfig);
            } else {
                return new MultipleInputInputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new MultipleInputInputSideHasNoUniqueKey(
                    ctx,
                    stateName,
                    inputSideSpec,
                    numberOfInputs,
                    operator,
                    multipleInputJoinEdges,
                    ttlConfig);
        }
    }

    public static AbstractKeyedBroadcastMultipleInputJoinRecordStateView createBroadcastState(
            RuntimeContext ctx,
            String stateName,
            MultipleInputJoinInputSideSpec inputSideSpec,
            int numberOfInputs,
            KeyedBroadcastMultipleInputStreamJoinOperator operator,
            MultipleInputJoinEdge[][] multipleInputJoinEdges,
            StateTtlConfig ttlConfig)
            throws Exception {
        return new MultipleInputInputSideBroadcast(
                ctx, stateName, inputSideSpec, operator, multipleInputJoinEdges, ttlConfig);
    }

    // ------------------------------------------------------------------------------------

    private static final class MultipleInputJoinKeyContainsUniqueKey
            extends AbstractKeyedBroadcastMultipleInputJoinRecordStateView {

        private final ValueState<RowData> recordState;
        private final List<RowData> reusedList;

        private MultipleInputJoinKeyContainsUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            ValueStateDescriptor<RowData> recordStateDesc =
                    new ValueStateDescriptor<>(stateName, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getState(recordStateDesc);
            // the result records always not more than 1
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            if (recordState.value() == null) {
                recordSize++;
            }
            recordState.update(record);
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            recordState.clear();
            long keyedSize = StreamSupport.stream(getRecords().spliterator(), false).count();
            recordSize -= keyedSize;
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            reusedList.clear();
            RowData record = recordState.value();
            if (record != null) {
                reusedList.add(record);
            }
            return reusedList;
        }

        @Override
        public Iterable<RowData> getRecords(RowData record, int inputIndex) {
            return null;
        }
    }

    private static final class MultipleInputInputSideHasUniqueKey
            extends AbstractKeyedBroadcastMultipleInputJoinRecordStateView {

        // stores record in the mapping <UK, Record>
        private final MapState<RowData, RowData> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;

        private MultipleInputInputSideHasUniqueKey(
                RuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> uniqueKeyType,
                KeySelector<RowData, RowData> uniqueKeySelector,
                StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    new MapStateDescriptor<>(stateName, uniqueKeyType, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            recordState.put(uniqueKey, record);
            if (!recordState.contains(uniqueKey)) {
                recordSize++;
            }
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            if (recordState.contains(uniqueKey)) {
                recordSize--;
            }
            recordState.remove(uniqueKey);
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return recordState.values();
        }

        @Override
        public Iterable<RowData> getRecords(RowData record, int inputIndex) {
            return null;
        }
    }

    private static final class MultipleInputInputSideHasNoUniqueKey
            extends AbstractKeyedBroadcastMultipleInputJoinRecordStateView {
        MultipleInputJoinEdge[][] multipleInputJoinEdges;
        private final MultipleInputJoinInputSideSpec inputSideSpec;
        private final MapState<RowData, Integer> recordState;
        private final KeyedBroadcastMultipleInputStreamJoinOperator operator;
        private final int numberOfInputs;
        private List<HashMap<RowData, List<RowData>>> indexs;
        private List<ListState<HashMap<RowData, List<RowData>>>> indexsStates;

        private MultipleInputInputSideHasNoUniqueKey(
                RuntimeContext ctx,
                String stateName,
                MultipleInputJoinInputSideSpec inputSideSpec,
                int numberOfInputs,
                KeyedBroadcastMultipleInputStreamJoinOperator operator,
                MultipleInputJoinEdge[][] multipleInputJoinEdges,
                StateTtlConfig ttlConfig)
                throws Exception {
            InternalTypeInfo<RowData> recordType = inputSideSpec.getInternalTypeInfo();
            MapStateDescriptor<RowData, Integer> recordStateDesc =
                    new MapStateDescriptor<>(stateName, recordType, Types.INT);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.multipleInputJoinEdges = multipleInputJoinEdges;
            this.operator = operator;
            this.recordState = ctx.getMapState(recordStateDesc);
            this.inputSideSpec = inputSideSpec;
            this.numberOfInputs = numberOfInputs;
            indexs = new ArrayList<>(Collections.nCopies(numberOfInputs, new HashMap<>()));
            initIndexState();
        }

        private void initIndexState() throws Exception {
            int inputIndex = inputSideSpec.getInputIndex();
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            InternalTypeInfo<RowData> stateKeyType = stateKeySelector.getProducedType();
            List<InternalTypeInfo<RowData>> indexKeyTypes =
                    new ArrayList<>(Collections.nCopies(numberOfInputs, null));
            List<RowDataKeySelector> keySelectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < numberOfInputs; i++) {
                RowDataKeySelector keySelector = keySelectors.get(i);
                if (keySelector != null && keySelector != stateKeySelector) {
                    indexKeyTypes.set(i, keySelector.getProducedType());
                }
            }
            List<ListState<HashMap<RowData, List<RowData>>>> indexsStates =
                    new ArrayList<>(Collections.nCopies(numberOfInputs, null));
            for (int i = 0; i < numberOfInputs; i++) {
                if (indexKeyTypes.get(i) == null) {
                    continue;
                }
                ListStateDescriptor<HashMap<RowData, List<RowData>>> indexStateDesc =
                        new ListStateDescriptor(
                                "indexe" + i,
                                Types.MAP(indexKeyTypes.get(i), Types.LIST(stateKeyType)));
                ListState<HashMap<RowData, List<RowData>>> indexState =
                        operator.getOperatorStateBackend().getListState(indexStateDesc);
                indexState.add(new HashMap<>());
                indexsStates.set(i, indexState);
            }
            this.indexsStates = indexsStates;
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                cnt += 1;
            } else {
                cnt = 1;
            }
            recordState.put(record, cnt);
            recordSize++;
            addToIndex(record);
        }

        private void addToIndex(RowData record) throws Exception {
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            RowData stateKey = stateKeySelector.getKey(record);
            List<int[]> joinKeyList = inputSideSpec.getJoinKeyList();
            List<RowDataKeySelector> selectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < joinKeyList.size(); i++) {
                int[] joinKey = joinKeyList.get(i);
                RowDataKeySelector selector = selectors.get(i);
                if (joinKey != null && selector != null && selector != stateKeySelector) {
                    RowData indexKey = selector.getKey(record);
                    Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                            indexsStates.get(i).get();
                    HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                    // HashMap<RowData, List<RowData>> indexMap = indexs.get(i);
                    if (indexMap.containsKey(indexKey)) {
                        indexMap.get(indexKey).add(stateKey);
                    } else {
                        List<RowData> list = new ArrayList<>();
                        list.add(stateKey);
                        indexMap.put(indexKey, list);
                    }
                }
            }
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            Integer cnt = recordState.get(record);
            if (cnt != null) {
                cnt -= 1;
                if (cnt <= 0) {
                    recordState.remove(record);
                    recordSize--;
                    removeFromIndex(record);
                } else {
                    recordState.put(record, cnt);
                }
            }
        }

        private void removeFromIndex(RowData record) throws Exception {
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            RowData stateKey = stateKeySelector.getKey(record);
            List<int[]> joinKeyList = inputSideSpec.getJoinKeyList();
            List<RowDataKeySelector> selectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < joinKeyList.size(); i++) {
                int[] joinKey = joinKeyList.get(i);
                RowDataKeySelector selector = selectors.get(i);
                if (joinKey != null && selector != null && selector != stateKeySelector) {
                    RowData indexKey = selector.getKey(record);
                    Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                            indexsStates.get(i).get();
                    HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                    if (!indexMap.containsKey(indexKey)) {
                        continue;
                    }
                    boolean isRemove = false;
                    List<RowData> rowDatas = indexMap.get(indexKey);
                    for (int j = 0; j < rowDatas.size(); j++) {
                        if (rowDatas.get(j).equals(stateKey)) {
                            rowDatas.remove(j);
                            isRemove = true;
                            break;
                        }
                    }
                    if (isRemove && rowDatas.isEmpty()) {
                        indexMap.remove(indexKey);
                    }
                }
            }
        }

        @Override
        public Iterable<RowData> getRecords(RowData record, int index) throws Exception {
            if (inputSideSpec.getKeySelectorList().get(index) == null) {
                return Collections.emptyList();
            }
            int stateIndex = inputSideSpec.getInputIndex();
            RowDataKeySelector leftSelector =
                    multipleInputJoinEdges[index][stateIndex].getLeftJoinKeySelector();
            RowData recordKey = leftSelector.getKey(record);
            List<RowData> stateKeyDataList = new ArrayList<>();
            int[] stateKey = inputSideSpec.getStateKey();
            int[] joinKey = inputSideSpec.getJoinKeyList().get(index);
            if (stateKey == joinKey) {
                stateKeyDataList.add(recordKey);
            } else {
                Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                        indexsStates.get(index).get();
                HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                stateKeyDataList = indexMap.getOrDefault(recordKey, null);
            }
            if (stateKeyDataList == null || stateKeyDataList.isEmpty()) {
                return Collections.emptyList();
            }
            List<RowData> records = new ArrayList<>();
            for (RowData stateKeyData : stateKeyDataList) {
                if (!((AbstractKeyedStateBackend<Object>) operator.getKeyedStateBackend())
                        .inCurrentKeyGroup(stateKeyData)) {
                    continue;
                }
                operator.setCurrentKey(stateKeyData);
                IterableIterator<RowData> iterator = new StateIteratorNoUniqueKey(recordState);
                List<RowData> list = IteratorUtils.toList(iterator);
                records.addAll(list);
            }
            return records;
        }

        private static class StateIteratorNoUniqueKey implements IterableIterator<RowData> {
            private final Iterator<Map.Entry<RowData, Integer>> backingIterable;
            private RowData record;
            private int remainingTimes = 0;

            private StateIteratorNoUniqueKey(MapState<RowData, Integer> recordState)
                    throws Exception {
                this.backingIterable = recordState.entries().iterator();
            }

            @Override
            public boolean hasNext() {
                return backingIterable.hasNext() || remainingTimes > 0;
            }

            @Override
            public RowData next() {
                if (remainingTimes > 0) {
                    checkNotNull(record);
                    remainingTimes--;
                } else {
                    Map.Entry<RowData, Integer> entry = backingIterable.next();
                    record = entry.getKey();
                    remainingTimes = entry.getValue() - 1;
                }
                return record;
            }

            @Override
            public Iterator<RowData> iterator() {
                return this;
            }
        };

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return null;
        }
    }

    private static final class MultipleInputInputSideBroadcast
            extends AbstractKeyedBroadcastMultipleInputJoinRecordStateView {
        private final MultipleInputJoinEdge[][] multipleInputJoinEdges;

        // stores record in the broadcastState
        private BroadcastState<RowData, Map<RowData, Integer>> broadcastState;
        private final MultipleInputJoinInputSideSpec inputSideSpec;
        private final KeyedBroadcastMultipleInputStreamJoinOperator operator;
        private List<HashMap<RowData, List<RowData>>> indexs;
        private List<ListState<HashMap<RowData, List<RowData>>>> indexsStates;

        private MultipleInputInputSideBroadcast(
                RuntimeContext ctx,
                String stateName,
                MultipleInputJoinInputSideSpec inputSideSpec,
                KeyedBroadcastMultipleInputStreamJoinOperator operator,
                MultipleInputJoinEdge[][] multipleInputJoinEdges,
                StateTtlConfig ttlConfig)
                throws Exception {
            this.inputSideSpec = inputSideSpec;
            this.operator = operator;
            indexs =
                    new ArrayList<>(
                            Collections.nCopies(multipleInputJoinEdges.length, new HashMap<>()));
            this.multipleInputJoinEdges = multipleInputJoinEdges;
            // initIndexState();
            initBroadcastState(stateName, inputSideSpec, ttlConfig);
            initIndexState();
        }

        private void initIndexState() throws Exception {
            int numberOfInputs = multipleInputJoinEdges.length;
            int inputIndex = inputSideSpec.getInputIndex();
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            InternalTypeInfo<RowData> stateKeyType = stateKeySelector.getProducedType();
            List<InternalTypeInfo<RowData>> indexKeyTypes =
                    new ArrayList<>(Collections.nCopies(numberOfInputs, null));
            List<RowDataKeySelector> keySelectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < numberOfInputs; i++) {
                RowDataKeySelector keySelector = keySelectors.get(i);
                if (keySelector != null && keySelector != stateKeySelector) {
                    indexKeyTypes.set(i, keySelector.getProducedType());
                }
            }
            List<ListState<HashMap<RowData, List<RowData>>>> indexsStates =
                    new ArrayList<>(Collections.nCopies(numberOfInputs, null));
            for (int i = 0; i < numberOfInputs; i++) {
                if (indexKeyTypes.get(i) == null) {
                    continue;
                }
                ListStateDescriptor<HashMap<RowData, List<RowData>>> indexStateDesc =
                        new ListStateDescriptor(
                                "indexe" + i,
                                Types.MAP(indexKeyTypes.get(i), Types.LIST(stateKeyType)));
                ListState<HashMap<RowData, List<RowData>>> indexState =
                        operator.getOperatorStateBackend().getListState(indexStateDesc);
                indexState.add(new HashMap<>());
                indexsStates.set(i, indexState);
            }
            this.indexsStates = indexsStates;
        }

        private void initBroadcastState(
                String stateName,
                MultipleInputJoinInputSideSpec inputSideSpec,
                StateTtlConfig ttlConfig) {
            InternalTypeInfo<RowData> recordType = inputSideSpec.getInternalTypeInfo();
            InternalTypeInfo<RowData> keyType =
                    inputSideSpec.getStateKeySelector().getProducedType();
            MapStateDescriptor<RowData, Map<RowData, Integer>> broadcastStateDescriptor =
                    new MapStateDescriptor<>(stateName, keyType, Types.MAP(recordType, Types.INT));
            if (ttlConfig.isEnabled()) {
                broadcastStateDescriptor.enableTimeToLive(ttlConfig);
            }
            try {
                this.broadcastState =
                        operator.getOperatorStateBackend()
                                .getBroadcastState(broadcastStateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addRecord(RowData record) throws Exception {
            RowData key = inputSideSpec.getStateKeySelector().getKey(record);
            Map<RowData, Integer> map = broadcastState.get(key);
            if (map != null) {
                Integer cnt = map.getOrDefault(record, 0);
                cnt++;
                map.put(record, cnt);
                broadcastState.put(key, map);
            } else {
                map = new HashMap<>();
                map.put(record, 1);
                broadcastState.put(key, map);
            }
            recordSize++;
            addToIndex(record);
        }

        private void addToIndex(RowData record) throws Exception {
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            RowData stateKey = stateKeySelector.getKey(record);
            List<int[]> joinKeyList = inputSideSpec.getJoinKeyList();
            List<RowDataKeySelector> selectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < joinKeyList.size(); i++) {
                int[] joinKey = joinKeyList.get(i);
                RowDataKeySelector selector = selectors.get(i);
                if (joinKey != null && selector != null && selector != stateKeySelector) {
                    RowData indexKey = selector.getKey(record);
                    Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                            indexsStates.get(i).get();
                    HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                    if (indexMap.containsKey(indexKey)) {
                        indexMap.get(indexKey).add(stateKey);
                    } else {
                        List<RowData> list = new ArrayList<>();
                        list.add(stateKey);
                        indexMap.put(indexKey, list);
                    }
                }
            }
        }

        @Override
        public void retractRecord(RowData record) throws Exception {
            RowData key = inputSideSpec.getStateKeySelector().getKey(record);
            Map<RowData, Integer> map = broadcastState.get(key);
            if (map != null) {
                Integer cnt = map.getOrDefault(record, 0);
                cnt--;
                if (cnt <= 0) {
                    broadcastState.remove(key);
                } else {
                    map.put(record, cnt);
                    broadcastState.put(key, map);
                }
                recordSize--;
                removeFromIndex(record);
            }
        }

        private void removeFromIndex(RowData record) throws Exception {
            RowDataKeySelector stateKeySelector = inputSideSpec.getStateKeySelector();
            RowData stateKey = stateKeySelector.getKey(record);
            List<int[]> joinKeyList = inputSideSpec.getJoinKeyList();
            List<RowDataKeySelector> selectors = inputSideSpec.getKeySelectorList();
            for (int i = 0; i < joinKeyList.size(); i++) {
                int[] joinKey = joinKeyList.get(i);
                RowDataKeySelector selector = selectors.get(i);
                if (joinKey != null && selector != null && selector != stateKeySelector) {
                    RowData indexKey = selector.getKey(record);
                    Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                            indexsStates.get(i).get();
                    HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                    if (!indexMap.containsKey(indexKey)) {
                        continue;
                    }
                    boolean isRemove = false;
                    List<RowData> rowDatas = indexMap.get(indexKey);
                    for (int j = 0; j < rowDatas.size(); j++) {
                        if (rowDatas.get(j).equals(stateKey)) {
                            rowDatas.remove(j);
                            isRemove = true;
                            break;
                        }
                    }
                    if (isRemove && rowDatas.isEmpty()) {
                        indexMap.remove(indexKey);
                    }
                }
            }
        }

        @Override
        public Iterable<RowData> getRecords() throws Exception {
            return null;
        }

        @Override
        public Iterable<RowData> getRecords(RowData record, int index) throws Exception {
            if (inputSideSpec.getKeySelectorList().get(index) == null) {
                return Collections.emptyList();
            }
            int stateIndex = inputSideSpec.getInputIndex();
            RowDataKeySelector leftSelector =
                    multipleInputJoinEdges[index][stateIndex].getLeftJoinKeySelector();
            RowData recordKey = leftSelector.getKey(record);
            List<RowData> stateKeyDataList = new ArrayList<>();
            int[] stateKey = inputSideSpec.getStateKey();
            int[] joinKey = inputSideSpec.getJoinKeyList().get(index);
            if (stateKey == joinKey) {
                stateKeyDataList.add(recordKey);
            } else {
                Iterable<HashMap<RowData, List<RowData>>> indexMapIter =
                        indexsStates.get(index).get();
                HashMap<RowData, List<RowData>> indexMap = indexMapIter.iterator().next();
                // HashMap<RowData, List<RowData>> indexMap = indexs.get(index);
                stateKeyDataList = indexMap.getOrDefault(recordKey, null);
            }
            if (stateKeyDataList == null) {
                return Collections.emptySet();
            }
            List<IterableIterator<RowData>> iterators = new ArrayList<>();
            for (RowData stateKeyData : stateKeyDataList) {
                Map<RowData, Integer> stateMap = broadcastState.get(stateKeyData);
                IterableIterator<RowData> iterator = new StateIteratorBroadcast(stateMap);
                iterators.add(iterator);
            }
            return new CombinedIterableIterator(iterators);
        }

        private class StateIteratorBroadcast implements IterableIterator<RowData> {

            private final Iterator<Map.Entry<RowData, Integer>> backingIterable;

            private RowData record;
            private int remainingTimes = 0;

            public StateIteratorBroadcast(Map<RowData, Integer> stateMap) throws Exception {
                if (stateMap == null) {
                    backingIterable = Collections.emptyIterator();
                } else {
                    backingIterable = stateMap.entrySet().iterator();
                }
            }

            @Override
            public boolean hasNext() {
                return backingIterable.hasNext() || remainingTimes > 0;
            }

            @Override
            public RowData next() {
                if (remainingTimes > 0) {
                    checkNotNull(record);
                    remainingTimes--;
                } else {
                    Map.Entry<RowData, Integer> entry = backingIterable.next();
                    record = entry.getKey();
                    remainingTimes = entry.getValue() - 1;
                }
                return record;
            }

            @Override
            public Iterator<RowData> iterator() {
                return this;
            }
        };
    }

    public static class CombinedIterableIterator implements IterableIterator<RowData> {
        private final List<IterableIterator<RowData>> iterators;
        private int currentIndex;

        public CombinedIterableIterator(List<IterableIterator<RowData>> iterators) {
            this.iterators = iterators;
            this.currentIndex = 0;
        }

        @Override
        public boolean hasNext() {
            while (currentIndex < iterators.size()) {
                if (iterators.get(currentIndex).hasNext()) {
                    return true;
                }
                currentIndex++;
            }
            return false;
        }

        @Override
        public RowData next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterators.get(currentIndex).next();
        }

        @Override
        public Iterator<RowData> iterator() {
            return this;
        }
    }
}
