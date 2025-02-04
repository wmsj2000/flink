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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.List;

/**
 * The factory to create {@link MultipleInputStreamJoinOperator}.
 *
 * @author Quentin Qiu
 */
public class MultipleInputStreamJoinOperatorFactory extends AbstractStreamOperatorFactory<RowData> {
    private static final long serialVersionUID = 1L;
    private final int numberOfInputs;
    private final List<JoinInputSideSpec> inputSideSpecs;
    private final List<InternalTypeInfo<RowData>> internalTypeInfos;
    private final long stateRetentionTime;
    private final List<List<GeneratedJoinCondition>> generatedJoinConditionsList;

    public MultipleInputStreamJoinOperatorFactory(
            int numberOfInputs,
            List<JoinInputSideSpec> inputSideSpecs,
            List<InternalTypeInfo<RowData>> internalTypeInfos,
            List<List<GeneratedJoinCondition>> generatedJoinConditionsList,
            long stateRetentionTime) {
        this.numberOfInputs = numberOfInputs;
        this.inputSideSpecs = inputSideSpecs;
        this.internalTypeInfos = internalTypeInfos;
        this.generatedJoinConditionsList = generatedJoinConditionsList;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public <T extends StreamOperator<RowData>> T createStreamOperator(
            StreamOperatorParameters<RowData> parameters) {
        return (T)
                new MultipleInputStreamJoinOperator(
                        parameters,
                        numberOfInputs,
                        inputSideSpecs,
                        internalTypeInfos,
                        generatedJoinConditionsList,
                        stateRetentionTime);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return MultipleInputStreamJoinOperator.class;
    }
}
