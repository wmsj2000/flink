package org.apache.flink.table.runtime.operators.join.stream.state;

/**
 * Abstract implementation for JoinRecordStateView which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractMultipleInputJoinRecordStateView implements JoinRecordStateView{
    protected Long recordSize = 0L;

    public Long getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(Long recordSize) {
        this.recordSize = recordSize;
    }
}
