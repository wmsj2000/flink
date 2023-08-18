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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.multiway.MultipleInputJoinEdge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The {@link MultipleInputJoinInputSideSpec} is ap specification which describes input side
 * information of a Join.
 *
 * @author Quentin Qiu
 */
public class MultipleInputJoinInputSideSpec implements Serializable {
    private static final long serialVersionUID = 3178408280297179599L;
    private final boolean inputSideIsBroadcast;
    private final int inputIndex;
    private int[] stateKey;
    private final List<int[]> joinKeyList;
    private RowDataKeySelector stateKeySelector;
    private final List<RowDataKeySelector> keySelectorList;
    private final InternalTypeInfo<RowData> internalTypeInfo;
    private boolean inputSideHasUniqueKey;
    private boolean joinKeyContainsUniqueKey;
    @Nullable private InternalTypeInfo<RowData> uniqueKeyType;
    @Nullable private RowDataKeySelector uniqueKeySelector;

    public MultipleInputJoinInputSideSpec(
            boolean inputSideIsBroadcast,
            InternalTypeInfo<RowData> internalTypeInfo,
            int inputIndex,
            List<int[]> joinKeyList,
            List<RowDataKeySelector> selectorList) {
        this.inputSideIsBroadcast = inputSideIsBroadcast;
        this.internalTypeInfo = internalTypeInfo;
        this.inputIndex = inputIndex;
        this.joinKeyList = joinKeyList;
        this.keySelectorList = selectorList;
        // tmp
        this.inputSideHasUniqueKey = false;
        this.joinKeyContainsUniqueKey = false;
        this.uniqueKeyType = null;
        this.uniqueKeySelector = null;
    }

    public void analyzeMultipleInputJoinInput(MultipleInputJoinEdge[][] multipleInputJoinEdges) {
        getStateKeyAndSelector();
    }

    public boolean isBroadcast() {
        return inputSideIsBroadcast;
    }

    private void getStateKeyAndSelector() {
        this.stateKey = joinKeyList.stream().filter(Objects::nonNull).findFirst().orElse(null);
        this.stateKeySelector =
                keySelectorList.stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    public RowDataKeySelector getStateKeySelector() {
        return stateKeySelector;
    }

    public int[] getStateKey() {
        return stateKey;
    }

    public List<int[]> getJoinKeyList() {
        return joinKeyList;
    }

    public List<RowDataKeySelector> getKeySelectorList() {
        return keySelectorList;
    }

    public InternalTypeInfo<RowData> getInternalTypeInfo() {
        return internalTypeInfo;
    }

    /** Returns true if the input has unique key, otherwise false. */
    public boolean hasUniqueKey() {
        return inputSideHasUniqueKey;
    }

    /** Returns true if the join key contains the unique key of the input. */
    public boolean joinKeyContainsUniqueKey() {
        return joinKeyContainsUniqueKey;
    }

    /**
     * Returns the {@link TypeInformation} of the unique key. Returns null if the input hasn't
     * unique key.
     */
    @Nullable
    public InternalTypeInfo<RowData> getUniqueKeyType() {
        return uniqueKeyType;
    }

    /**
     * Returns the {@link KeySelector} to extract unique key from the input row. Returns null if the
     * input hasn't unique key.
     */
    @Nullable
    public KeySelector<RowData, RowData> getUniqueKeySelector() {
        return uniqueKeySelector;
    }

    @Override
    public String toString() {
        if (inputSideHasUniqueKey) {
            if (joinKeyContainsUniqueKey) {
                return "JoinKeyContainsUniqueKey";
            } else {
                return "HasUniqueKey";
            }
        } else {
            return "NoUniqueKey";
        }
    }

    public int getInputIndex() {
        return inputIndex;
    }
}
