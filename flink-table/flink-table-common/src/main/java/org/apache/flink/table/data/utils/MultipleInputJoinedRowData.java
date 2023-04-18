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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link RowData} which is backed by multiple concatenated {@link RowData}.
 *
 * @author Quentin Qiu
 */
@PublicEvolving
public class MultipleInputJoinedRowData implements RowData {
    private RowKind rowKind = RowKind.INSERT;
    private List<RowData> rows;
    private List<Integer> prefixSumOfArity;

    public MultipleInputJoinedRowData() {}

    public MultipleInputJoinedRowData(List<RowData> rows) {
        this.rows = rows;
        this.prefixSumOfArity = getPrefixSum(rows);
    }

    public MultipleInputJoinedRowData(RowKind kind, List<RowData> rows) {
        this.rowKind = kind;
        this.rows = rows;
        this.prefixSumOfArity = getPrefixSum(rows);
    }

    public MultipleInputJoinedRowData replace(List<RowData> rows) {
        this.rows = rows;
        this.prefixSumOfArity = getPrefixSum(rows);
        return this;
    }

    private List<Integer> getPrefixSum(List<RowData> rows) {
        List<Integer> prefixSumOfArity = new ArrayList<>();
        prefixSumOfArity.add(rows.get(0).getArity());
        for (int i = 1; i < rows.size(); i++) {
            prefixSumOfArity.add(prefixSumOfArity.get(i - 1) + rows.get(i).getArity());
        }
        return prefixSumOfArity;
    }

    private Pair<Integer, Integer> getPositionIndex(int pos) {
        if (pos > prefixSumOfArity.get(prefixSumOfArity.size() - 1)) {
            return Pair.of(
                    prefixSumOfArity.size() - 1,
                    pos - prefixSumOfArity.get(prefixSumOfArity.size() - 2));
        } else if (pos < 0) {
            return Pair.of(0, pos);
        } else {
            for (int i = 0; i < prefixSumOfArity.size(); i++) {
                if (prefixSumOfArity.get(i) > pos) {
                    int newPos = i > 0 ? pos - prefixSumOfArity.get(i - 1) : pos;
                    return Pair.of(i, newPos);
                }
            }
        }
        return null;
    }

    @Override
    public int getArity() {
        int sumOfArity = 0;
        for (RowData row : rows) {
            sumOfArity += row.getArity();
        }
        return sumOfArity;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).isNullAt(pair.getValue());
    }

    @Override
    public boolean getBoolean(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getBoolean(pair.getValue());
    }

    @Override
    public byte getByte(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getByte(pair.getValue());
    }

    @Override
    public short getShort(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getShort(pair.getValue());
    }

    @Override
    public int getInt(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getInt(pair.getValue());
    }

    @Override
    public long getLong(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getLong(pair.getValue());
    }

    @Override
    public float getFloat(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getFloat(pair.getValue());
    }

    @Override
    public double getDouble(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getDouble(pair.getValue());
    }

    @Override
    public StringData getString(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getString(pair.getValue());
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getDecimal(pair.getValue(), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getTimestamp(pair.getValue(), precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getRawValue(pair.getValue());
    }

    @Override
    public byte[] getBinary(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getBinary(pair.getValue());
    }

    @Override
    public ArrayData getArray(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getArray(pair.getValue());
    }

    @Override
    public MapData getMap(int pos) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getMap(pair.getValue());
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        Pair<Integer, Integer> pair = getPositionIndex(pos);
        assert pair != null;
        return rows.get(pair.getKey()).getRow(pair.getValue(), numFields);
    }
}
