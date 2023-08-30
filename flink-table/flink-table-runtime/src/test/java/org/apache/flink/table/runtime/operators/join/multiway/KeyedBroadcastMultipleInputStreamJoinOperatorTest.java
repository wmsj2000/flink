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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorTest;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

/** Tests for the facilities provided by {@link AbstractStreamOperatorV2}. */
public class KeyedBroadcastMultipleInputStreamJoinOperatorTest extends AbstractStreamOperatorTest {
    @Test
    public void testMultipleInputStreamJoin() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedBroadcastMultiInputStreamJoinOperatorTestHarness testHarness =
                new KeyedBroadcastMultiInputStreamJoinOperatorTestHarness<>(
                        new KeyedBroadcastMultipleInputStreamJoinOperatorFactory(
                                0, null, null, null, 0),
                        BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setKeySelector(0, dummyKeySelector);
            testHarness.setKeySelector(1, dummyKeySelector);
            testHarness.setKeySelector(2, dummyKeySelector);
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(0, new StreamRecord<>(1L, 1L));
            testHarness.processElement(0, new StreamRecord<>(3L, 3L));
            testHarness.processElement(0, new StreamRecord<>(4L, 4L));
            // assertThat(testHarness.getOutput(), empty());
        }
    }
}
