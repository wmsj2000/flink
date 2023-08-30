package org.apache.flink.table.runtime.operators.join.multiway;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorTest;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/** Tests for the facilities provided by {@link AbstractStreamOperatorV2}. */
public class KeyedBroadcastMultipleInputStreamJoinOperatorTest extends AbstractStreamOperatorTest {
    @Test
    public void testMultipleInputStreamJoin() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        try (KeyedBroadcastMultiInputStreamJoinOperatorTestHarness testHarness =
                     new KeyedBroadcastMultiInputStreamJoinOperatorTestHarness<>(
                             new KeyedBroadcastMultipleInputStreamJoinOperatorFactory(0,null,null,null,0),
                             BasicTypeInfo.INT_TYPE_INFO)) {
            testHarness.setKeySelector(0, dummyKeySelector);
            testHarness.setKeySelector(1, dummyKeySelector);
            testHarness.setKeySelector(2, dummyKeySelector);
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(0, new StreamRecord<>(1L, 1L));
            testHarness.processElement(0, new StreamRecord<>(3L, 3L));
            testHarness.processElement(0, new StreamRecord<>(4L, 4L));
            //assertThat(testHarness.getOutput(), empty());
        }
    }

}
