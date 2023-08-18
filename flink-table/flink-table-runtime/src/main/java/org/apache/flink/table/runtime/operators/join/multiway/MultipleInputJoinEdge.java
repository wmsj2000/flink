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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.Serializable;
import java.util.List;

/** @author Quentin Qiu */
public class MultipleInputJoinEdge implements Serializable {
    private static final long serialVersionUID = 3178408026497179959L;
    private final GeneratedJoinCondition generatedJoinCondition;
    private final RowDataKeySelector leftJoinKeySelector;
    private final RowDataKeySelector rightJoinKeySelector;
    private final InternalTypeInfo<RowData> leftInternalTypeInfo;
    private final InternalTypeInfo<RowData> rightInternalTypeInfo;
    private final FlinkJoinType joinType;
    /** default leftJoinKeys.size==1 */
    private final int leftInputIndex;

    private final int rightInputIndex;
    private final int[] leftJoinKey;
    private final int[] rightJoinKey;
    private final List<int[]> leftUpsertKey;
    private final List<int[]> rightUpsertKey;
    private final boolean[] filterNulls;

    public MultipleInputJoinEdge(
            GeneratedJoinCondition generatedJoinCondition,
            FlinkJoinType joinType,
            int leftInputIndex,
            int rightInputIndex,
            int[] leftJoinKey,
            int[] rightJoinKey,
            List<int[]> leftUpsertKey,
            List<int[]> rightUpsertKey,
            InternalTypeInfo<RowData> leftInternalTypeInfo,
            InternalTypeInfo<RowData> rightInternalTypeInfo,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            boolean[] filterNulls) {
        this.generatedJoinCondition = generatedJoinCondition;
        this.joinType = joinType;
        this.leftInputIndex = leftInputIndex;
        this.rightInputIndex = rightInputIndex;
        this.leftJoinKey = leftJoinKey;
        this.rightJoinKey = rightJoinKey;
        this.leftUpsertKey = leftUpsertKey;
        this.rightUpsertKey = rightUpsertKey;
        this.leftInternalTypeInfo = leftInternalTypeInfo;
        this.rightInternalTypeInfo = rightInternalTypeInfo;
        this.leftJoinKeySelector = leftJoinKeySelector;
        this.rightJoinKeySelector = rightJoinKeySelector;
        this.filterNulls = filterNulls;
    }

    public GeneratedJoinCondition getGeneratedJoinCondition() {
        return generatedJoinCondition;
    }

    public FlinkJoinType getJoinType() {
        return joinType;
    }

    public int getLeftInputIndex() {
        return leftInputIndex;
    }

    public int getRightInputIndex() {
        return rightInputIndex;
    }

    public int[] getLeftJoinKey() {
        return leftJoinKey;
    }

    public int[] getRightJoinKey() {
        return rightJoinKey;
    }

    public List<int[]> getLeftUpsertKey() {
        return leftUpsertKey;
    }

    public List<int[]> getRightUpsertKey() {
        return rightUpsertKey;
    }

    public boolean[] getFilterNulls() {
        return filterNulls;
    }

    public RowDataKeySelector getLeftJoinKeySelector() {
        return leftJoinKeySelector;
    }

    public RowDataKeySelector getRightJoinKeySelector() {
        return rightJoinKeySelector;
    }

    public InternalTypeInfo<RowData> getLeftInternalTypeInfo() {
        return leftInternalTypeInfo;
    }

    public InternalTypeInfo<RowData> getRightInternalTypeInfo() {
        return rightInternalTypeInfo;
    }
}
