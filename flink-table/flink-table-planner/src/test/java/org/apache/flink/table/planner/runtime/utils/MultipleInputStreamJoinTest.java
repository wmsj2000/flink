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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

/** MultipleInputStreamJoinTest */
public class MultipleInputStreamJoinTest extends TestLogger {

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
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, false);
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table1 (\n"
                                + "  id INT,\n"
                                + "  age INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'rows-per-second'='10',\n"
                                + "  'fields.id.kind'='sequence' ,\n"
                                + "  'fields.id.start'='1',\n"
                                + "  'fields.id.end'='100'\n"
                                + ");"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table1"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table2 (\n"
                                + "  id INT,\n"
                                + "  age INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'rows-per-second'='10',\n"
                                + "  'fields.id.kind'='sequence' ,\n"
                                + "  'fields.id.start'='1',\n"
                                + "  'fields.id.end'='100'\n"
                                + ");"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table2"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table3 (\n"
                                + "  id INT,\n"
                                + "  age INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'rows-per-second'='10',\n"
                                + "  'fields.id.kind'='sequence' ,\n"
                                + "  'fields.id.start'='1',\n"
                                + "  'fields.id.end'='100'\n"
                                + ");"));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "source_table3"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table4 (\n"
                                + "  id INT,\n"
                                + "  age INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'datagen',\n"
                                + "  'rows-per-second'='10',\n"
                                + "  'fields.id.kind'='sequence' ,\n"
                                + "  'fields.id.start'='1',\n"
                                + "  'fields.id.end'='100'\n"
                                + ");"));
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

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testMultipleJoinsTest(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED,
                        multipleJoinEnable);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        String query =
                "SELECT * FROM T4 "
                        + "JOIN T3 ON T4.d4 = T3.c3 "
                        + "JOIN T2 ON T3.b3 = T2.b2 "
                        + "JOIN T1 ON T2.a2 = T1.a1";
        String query1 =
                "SELECT * FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1";
        // System.out.println(tEnv.explainSql(query));
        tEnv.executeSql(query1).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testJoins2Test(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED,
                        multipleJoinEnable);
        Configuration conf = new Configuration();
        conf.setString("adaptive", "true");
        conf.setString("period", "60");
        conf.setString("delay", "30");
        conf.setString("collect", "5");
        conf.setString("reset", "false");
        env.getConfig().setGlobalJobParameters(conf);
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        String query1 =
                "SELECT * \n"
                        + "FROM source_table1\n"
                        + "JOIN source_table2 ON source_table1.id = source_table2.id\n"
                        + "JOIN source_table3 ON source_table2.id = source_table3.id\n"
                        + "JOIN source_table4 ON source_table3.id = source_table4.id;";
        System.out.println(tEnv.explainSql(query1));
        tEnv.executeSql(query1).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testJoins3Test(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED,
                        multipleJoinEnable);
        String query1 =
                "SELECT * \n"
                        + "FROM source_table3\n"
                        + "JOIN source_table1 ON source_table3.id = source_table1.id\n"
                        + "JOIN source_table4 ON source_table3.id = source_table4.id\n"
                        + "JOIN source_table2 ON source_table3.id = source_table2.id;";
        System.out.println(tEnv.explainSql(query1));
        tEnv.executeSql(query1).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testJoins3(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED,
                        multipleJoinEnable);
        Configuration conf = new Configuration();
        conf.setString("adaptive", "true");
        conf.setString("period", "30");
        conf.setString("delay", "0");
        conf.setString("collect", "5");
        conf.setString("reset", "false");
        env.getConfig().setGlobalJobParameters(conf);
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        tEnv.executeSql(
                "CREATE TABLE source_table11 (\n"
                        + "  id INT,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='50',\n"
                        + "  'fields.id.kind'='sequence' ,\n"
                        + "  'fields.id.start'='1',\n"
                        + "  'fields.id.end'='1000'\n"
                        + ");\n");
        tEnv.executeSql(
                "CREATE TABLE source_table21 (\n"
                        + "  id INT,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='50',\n"
                        + "  'fields.id.kind'='sequence' ,\n"
                        + "  'fields.id.start'='1',\n"
                        + "  'fields.id.end'='1000'\n"
                        + ");\n");
        tEnv.executeSql(
                "CREATE TABLE source_table31 (\n"
                        + "  id INT,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='50',\n"
                        + "  'fields.id.kind'='sequence' ,\n"
                        + "  'fields.id.start'='1',\n"
                        + "  'fields.id.end'='1000'\n"
                        + ");\n");
        tEnv.executeSql(
                "CREATE TABLE source_table41 (\n"
                        + "  id INT,\n"
                        + "  age INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='50',\n"
                        + "  'fields.id.kind'='sequence' ,\n"
                        + "  'fields.id.start'='1',\n"
                        + "  'fields.id.end'='1000'\n"
                        + ");");
        String query1 =
                "SELECT * \n"
                        + "FROM source_table11\n"
                        + "JOIN source_table21 ON source_table11.id = source_table21.id\n"
                        + "JOIN source_table31 ON source_table21.id = source_table31.id\n"
                        + "JOIN source_table41 ON source_table31.id = source_table41.id;";
        System.out.println(tEnv.explainSql(query1));
        tEnv.executeSql(query1).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testQ2mj(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED, true);
        createTables(tEnv);
        // 获取注册的配置
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        String data = map.getOrDefault("data", "1g");
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "Q2mj");

        String query =
                "SELECT * "
                        + "FROM supplier, partsupp, nation, region "
                        + "WHERE supplier.s_suppkey = partsupp.ps_suppkey "
                        + "AND supplier.s_nationkey = nation.n_nationkey "
                        + "AND nation.n_regionkey = region.r_regionkey;";
        String query1 =
                "SELECT s_acctbal,s_name,n_name,s_address, s_phone,s_comment \n "
                        + "FROM supplier, partsupp, nation, region "
                        + "WHERE supplier.s_suppkey = partsupp.ps_suppkey "
                        + "AND supplier.s_nationkey = nation.n_nationkey "
                        + "AND nation.n_regionkey = region.r_regionkey;";
        System.out.println(tEnv.explainSql(query1));
        tEnv.executeSql(query1).print();
        // tEnv.executeSql("INSERT INTO sink_table_query " + query);

    }

    public void createTables(TableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE customer (\n"
                        + "    c_custkey INT,\n"
                        + "    c_name VARCHAR(25),\n"
                        + "    c_address VARCHAR(40),\n"
                        + "    c_nationkey INT,\n"
                        + "    c_phone CHAR(15),\n"
                        + "    c_acctbal DECIMAL(15,2),\n"
                        + "    c_mktsegment CHAR(10),\n"
                        + "    c_comment VARCHAR(117)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.c_custkey.kind'='sequence' ,\n"
                        + "  'fields.c_custkey.start'='1',\n"
                        + "  'fields.c_custkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE lineitem (\n"
                        + "    l_orderkey INT,\n"
                        + "    l_partkey INT,\n"
                        + "    l_suppkey INT,\n"
                        + "    l_linenumber INT,\n"
                        + "    l_quantity DECIMAL(15,2),\n"
                        + "    l_extendedprice DECIMAL(15,2),\n"
                        + "    l_discount DECIMAL(15,2),\n"
                        + "    l_tax DECIMAL(15,2),\n"
                        + "    l_returnflag CHAR(1),\n"
                        + "    l_linestatus CHAR(1),\n"
                        + "    l_shipdate DATE,\n"
                        + "    l_commitdate DATE,\n"
                        + "    l_receiptdate DATE,\n"
                        + "    l_shipinstruct CHAR(25),\n"
                        + "    l_shipmode CHAR(10),\n"
                        + "    l_comment VARCHAR(44)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.l_orderkey.kind'='sequence' ,\n"
                        + "  'fields.l_orderkey.start'='1',\n"
                        + "  'fields.l_orderkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE nation (\n"
                        + "    n_nationkey INT,\n"
                        + "    n_name CHAR(25),\n"
                        + "    n_regionkey INT,\n"
                        + "    n_comment VARCHAR(152)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.n_nationkey.kind'='sequence' ,\n"
                        + "  'fields.n_nationkey.start'='1',\n"
                        + "  'fields.n_nationkey.end'='1000',\n"
                        + "  'fields.n_regionkey.kind'='sequence' ,\n"
                        + "  'fields.n_regionkey.start'='1',\n"
                        + "  'fields.n_regionkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE orders (\n"
                        + "    o_orderkey INT,\n"
                        + "    o_custkey INT,\n"
                        + "    o_orderstatus CHAR(1),\n"
                        + "    o_totalprice DECIMAL(15,2),\n"
                        + "    o_orderdate DATE,\n"
                        + "    o_orderpriority CHAR(15),\n"
                        + "    o_clerk CHAR(15),\n"
                        + "    o_shippriority INT,\n"
                        + "    o_comment VARCHAR(79)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.o_orderkey.kind'='sequence' ,\n"
                        + "  'fields.o_orderkey.start'='1',\n"
                        + "  'fields.o_orderkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE part (\n"
                        + "    p_partkey INT,\n"
                        + "    p_name VARCHAR(55),\n"
                        + "    p_mfgr CHAR(25),\n"
                        + "    p_brand CHAR(10),\n"
                        + "    p_type VARCHAR(25),\n"
                        + "    p_size INT,\n"
                        + "    p_container CHAR(10),\n"
                        + "    p_retailprice DECIMAL(15,2),\n"
                        + "    p_comment VARCHAR(23)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.p_partkey.kind'='sequence' ,\n"
                        + "  'fields.p_partkey.start'='1',\n"
                        + "  'fields.p_partkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE partsupp (\n"
                        + "    ps_partkey INT,\n"
                        + "    ps_suppkey INT,\n"
                        + "    ps_availqty INT,\n"
                        + "    ps_supplycost DECIMAL(15,2),\n"
                        + "    ps_comment VARCHAR(199)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.ps_partkey.kind'='sequence' ,\n"
                        + "  'fields.ps_partkey.start'='1',\n"
                        + "  'fields.ps_partkey.end'='1000',\n"
                        + "  'fields.ps_suppkey.kind'='sequence' ,\n"
                        + "  'fields.ps_suppkey.start'='1',\n"
                        + "  'fields.ps_suppkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE region (\n"
                        + "    r_regionkey INT,\n"
                        + "    r_name CHAR(25),\n"
                        + "    r_comment VARCHAR(152)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.r_regionkey.kind'='sequence' ,\n"
                        + "  'fields.r_regionkey.start'='1',\n"
                        + "  'fields.r_regionkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE supplier (\n"
                        + "    s_suppkey INT,\n"
                        + "    s_name CHAR(25),\n"
                        + "    s_address VARCHAR(40),\n"
                        + "    s_nationkey INT,\n"
                        + "    s_phone CHAR(15),\n"
                        + "    s_acctbal DECIMAL(15,2),\n"
                        + "    s_comment VARCHAR(101)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='10',\n"
                        + "  'fields.s_suppkey.kind'='sequence' ,\n"
                        + "  'fields.s_suppkey.start'='1',\n"
                        + "  'fields.s_suppkey.end'='1000',\n"
                        + "  'fields.s_nationkey.kind'='sequence' ,\n"
                        + "  'fields.s_nationkey.start'='1',\n"
                        + "  'fields.s_nationkey.end'='1000'\n"
                        + ");");
    }
}
