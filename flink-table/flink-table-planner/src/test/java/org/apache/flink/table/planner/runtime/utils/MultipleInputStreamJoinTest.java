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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

/** MultipleInputStreamJoinTest */
public abstract class MultipleInputStreamJoinTest extends TestLogger {

    private static final int DEFAULT_PARALLELISM = 4;

    protected TableEnvironment tEnv;
    private Catalog catalog;
    private StreamExecutionEnvironment env;

    protected void assertEquals(String query, List<String> expectedList) {
        StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tEnv;
        Table table = streamTableEnvironment.sqlQuery(query);
        TestingRetractSink sink = new TestingRetractSink();
        streamTableEnvironment
                .toRetractStream(table, Row.class)
                .map(JavaScalaConversionUtil::toScala, TypeInformation.of(Tuple2.class))
                .addSink((SinkFunction) sink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<String> results = JavaScalaConversionUtil.toJava(sink.getRetractResults());
        results = new ArrayList<>(results);
        results.sort(String::compareTo);
        expectedList.sort(String::compareTo);

        assertThat(results).isEqualTo(expectedList);
    }

    protected TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(env, settings);
    }

    @BeforeEach
    public void before() throws Exception {
        tEnv = getTableEnvironment();
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();

        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table1 (id INT,name STRING) WITH ('connector' = 'datagen','fields.id.min'='1','fields.id.max'='1000');"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table1"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table2 (id INT,name STRING) WITH ('connector' = 'datagen','fields.id.min'='1','fields.id.max'='1000');"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table2"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table3 (id INT,name STRING) WITH ('connector' = 'datagen','fields.id.min'='1','fields.id.max'='1000');"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table3"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table4 (id INT,name STRING) WITH ('connector' = 'datagen','fields.id.min'='1','fields.id.max'='1000');"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table4"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        // Test data
        String dataId2 = TestValuesTableFactory.registerData(TestData.data2());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T1 (\n"
                                + "  a1 INT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 INT,\n"
                                + "  d1 STRING,\n"
                                + "  e1 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T1"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T2 (\n"
                                + "  a2 INT,\n"
                                + "  b2 BIGINT,\n"
                                + "  c2 INT,\n"
                                + "  d2 STRING,\n"
                                + "  e2 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T2"),
                new CatalogTableStatistics(10000, 1, 1, 1),
                false);

        String dataId3 = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T3 (\n"
                                + "  a3 INT,\n"
                                + "  b3 BIGINT,\n"
                                + "  c3 STRING\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId3));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T3"),
                new CatalogTableStatistics(1000, 1, 1, 1),
                false);

        String dataId5 = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T4 (\n"
                                + "  a4 INT,\n"
                                + "  b4 BIGINT,\n"
                                + "  c4 INT,\n"
                                + "  d4 STRING,\n"
                                + "  e4 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId5));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T4"),
                new CatalogTableStatistics(100, 1, 1, 1),
                false);
    }

    @AfterEach
    public void after() {
        TestValuesTableFactory.clearAllData();
        StreamTestSink.clear();
    }

    @Test
    public void testMultipleJoinsTest() {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);
        String query =
                "SELECT * FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1";
        System.out.println(tEnv.explainSql(query));
        tEnv.executeSql(query).print();
    }

    @Test
    public void testJoins2Test() {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);
        String query1 =
                "SELECT * \n"
                        + "FROM source_table1\n"
                        + "JOIN source_table2 ON source_table1.id = source_table2.id\n"
                        + "JOIN source_table3 ON source_table2.id = source_table3.id\n"
                        + "JOIN source_table4 ON source_table3.id = source_table4.id;";
        System.out.println(tEnv.explainSql(query1));
        tEnv.executeSql(query1).print();
    }
}
