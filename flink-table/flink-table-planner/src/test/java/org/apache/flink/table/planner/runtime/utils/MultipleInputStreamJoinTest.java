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
import java.util.HashMap;
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
    public void testTpchQ2(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED, true);
        // 获取注册的配置
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        String data = map.getOrDefault("data", "1g");
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "tpch");
        createTables(tEnv);
        String Q7 =
                "                select\n"
                        + "                        n1.n_name as supp_nation,\n"
                        + "                        n2.n_name as cust_nation\n"
                        + "                from\n"
                        + "                        supplier,\n"
                        + "                        lineitem,\n"
                        + "                        orders,\n"
                        + "                        customer,\n"
                        + "                        nation n1,\n"
                        + "                        nation n2\n"
                        + "                where\n"
                        + "                        s_suppkey = l_suppkey\n"
                        + "                        and o_orderkey = l_orderkey\n"
                        + "                        and c_custkey = o_custkey\n"
                        + "                        and s_nationkey = n1.n_nationkey\n"
                        + "                        and c_nationkey = n2.n_nationkey\n";
        System.out.println(tEnv.explainSql(Q7));
        tEnv.executeSql(Q7).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testTpchQ5(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED, true);
        // 获取注册的配置
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        String data = map.getOrDefault("data", "1g");
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "tpch");
        createTables(tEnv);
        String Q5 =
                "select\n"
                        + "        c_custkey, \n"
                        + "        l_orderkey, \n"
                        + "        l_suppkey, \n"
                        + "        c_nationkey, \n"
                        + "        n_regionkey \n"
                        + "from\n"
                        + "        customer,\n"
                        + "        orders,\n"
                        + "        lineitem,\n"
                        + "        supplier,\n"
                        + "        nation,\n"
                        + "        region\n"
                        + "where\n"
                        + "        c_custkey = o_custkey\n"
                        + "        and l_orderkey = o_orderkey\n"
                        + "        and l_suppkey = s_suppkey\n"
                        + "        and c_nationkey = s_nationkey\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_regionkey = r_regionkey\n"
                ;
    System.out.println(tEnv.explainSql(Q5));
    tEnv.executeSql(Q5).print();
    }

    @ParameterizedTest(name = "Is MultipleInputJoin open: {0}")
    @ValueSource(booleans = {true})
    public void testTpch(boolean multipleJoinEnable) {
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_JOIN_ENABLED, true);
        // 获取注册的配置
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        String data = map.getOrDefault("data", "1g");
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(24));
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "tpch");
        createTables(tEnv);
        String query =
                "SELECT s_acctbal,s_name,n_name,s_address, s_phone,s_comment \n "
                        + "FROM supplier, partsupp, nation, region "
                        + "WHERE supplier.s_suppkey = partsupp.ps_suppkey "
                        + "AND supplier.s_nationkey = nation.n_nationkey "
                        + "AND nation.n_regionkey = region.r_regionkey;";
        HashMap<String, String> queriesMap = getTpchQueries();
        HashMap<String, String> sinkTableMap = getSinkTables();
        String number = "7";
        query = queriesMap.get("Q" + number);
        tEnv.executeSql(sinkTableMap.get("Q" + number));
        String queryToSink = "Insert into sink_table_q" + number + " \n" + query;
        System.out.println(tEnv.explainSql(queryToSink));
        tEnv.executeSql(queryToSink).print();
        tEnv.executeSql(query).print();
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
                        + "  'rows-per-second'='100',\n"
                        + "  'fields.c_custkey.kind'='sequence' ,\n"
                        + "  'fields.c_custkey.start'='1',\n"
                        + "  'fields.c_custkey.end'='1000',\n"
                        + "  'fields.c_nationkey.kind'='sequence' ,\n"
                        + "  'fields.c_nationkey.start'='1',\n"
                        + "  'fields.c_nationkey.end'='1000'\n"
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
                        + "  'rows-per-second'='100',\n"
                        + "  'fields.l_orderkey.kind'='sequence' ,\n"
                        + "  'fields.l_orderkey.start'='1',\n"
                        + "  'fields.l_orderkey.end'='1000',\n"
                        + "  'fields.l_suppkey.kind'='sequence' ,\n"
                        + "  'fields.l_suppkey.start'='1',\n"
                        + "  'fields.l_suppkey.end'='1000'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TABLE nation (\n"
                        + "    n_nationkey INT,\n"
                        + "    n_name CHAR(25),\n"
                        + "    n_regionkey INT,\n"
                        + "    n_comment VARCHAR(152)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second'='100',\n"
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
                        + "  'rows-per-second'='100',\n"
                        + "  'fields.o_orderkey.kind'='sequence' ,\n"
                        + "  'fields.o_orderkey.start'='1',\n"
                        + "  'fields.o_orderkey.end'='1000',\n"
                        + "  'fields.o_custkey.kind'='sequence' ,\n"
                        + "  'fields.o_custkey.start'='1',\n"
                        + "  'fields.o_custkey.end'='1000'\n"
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
                        + "  'rows-per-second'='100',\n"
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
                        + "  'rows-per-second'='100',\n"
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
                        + "  'rows-per-second'='100',\n"
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
                        + "  'rows-per-second'='100',\n"
                        + "  'fields.s_suppkey.kind'='sequence' ,\n"
                        + "  'fields.s_suppkey.start'='1',\n"
                        + "  'fields.s_suppkey.end'='1000',\n"
                        + "  'fields.s_nationkey.kind'='sequence' ,\n"
                        + "  'fields.s_nationkey.start'='1',\n"
                        + "  'fields.s_nationkey.end'='1000'\n"
                        + ");");
    }

    public HashMap<String, String> getTpchQueries() {
        HashMap<String, String> map = new HashMap<>();
        String Q22 =
                "select\n"
                        + "        cntrycode,\n"
                        + "        count(*) as numcust,\n"
                        + "        sum(c_acctbal) as totacctbal\n"
                        + "from\n"
                        + "        (\n"
                        + "                select\n"
                        + "                        substring(c_phone from 1 for 2) as cntrycode,\n"
                        + "                        c_acctbal\n"
                        + "                from\n"
                        + "                        customer\n"
                        + "                where\n"
                        + "                        substring(c_phone from 1 for 2) in\n"
                        + "                                ('24', '32', '17', '18', '12', '14', '22')\n"
                        + "                        and c_acctbal > (\n"
                        + "                                select\n"
                        + "                                        avg(c_acctbal)\n"
                        + "                                from\n"
                        + "                                        customer\n"
                        + "                                where\n"
                        + "                                        c_acctbal > 0.00\n"
                        + "                                        and substring(c_phone from 1 for 2) in\n"
                        + "                                                ('24', '32', '17', '18', '12', '14', '22')\n"
                        + "                        )\n"
                        + "                        and not exists (\n"
                        + "                                select\n"
                        + "                                        *\n"
                        + "                                from\n"
                        + "                                        orders\n"
                        + "                                where\n"
                        + "                                        o_custkey = c_custkey\n"
                        + "                        )\n"
                        + "        ) as custsale\n"
                        + "group by\n"
                        + "        cntrycode;";
        String Q21 =
                "select\n"
                        + "        s_name,\n"
                        + "        count(*) as numwait\n"
                        + "from\n"
                        + "        lineitem l1,\n"
                        + "        orders,\n"
                        + "        supplier,\n"
                        + "        nation\n"
                        + "where\n"
                        + "        s_suppkey = l1.l_suppkey\n"
                        + "        and o_orderkey = l1.l_orderkey\n"
                        + "        and o_orderstatus = 'F'\n"
                        + "        and l1.l_receiptdate > l1.l_commitdate\n"
                        + "        and exists (\n"
                        + "                select\n"
                        + "                        *\n"
                        + "                from\n"
                        + "                        lineitem l2\n"
                        + "                where\n"
                        + "                        l2.l_orderkey = l1.l_orderkey\n"
                        + "                        and l2.l_suppkey <> l1.l_suppkey\n"
                        + "        )\n"
                        + "        and not exists (\n"
                        + "                select\n"
                        + "                        *\n"
                        + "                from\n"
                        + "                        lineitem l3\n"
                        + "                where\n"
                        + "                        l3.l_orderkey = l1.l_orderkey\n"
                        + "                        and l3.l_suppkey <> l1.l_suppkey\n"
                        + "                        and l3.l_receiptdate > l3.l_commitdate\n"
                        + "        )\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_name = 'PERU'\n"
                        + "group by\n"
                        + "        s_name;";
        String Q20 =
                "select\n"
                        + "        s_name,\n"
                        + "        s_address\n"
                        + "from\n"
                        + "        supplier,\n"
                        + "        nation\n"
                        + "where\n"
                        + "        s_suppkey in (\n"
                        + "          select\n"
                        + "          ps_suppkey\n"
                        + "          from\n"
                        + "          partsupp\n"
                        + "          where\n"
                        + "          ps_partkey in (\n"
                        + "            select\n"
                        + "            p_partkey\n"
                        + "            from\n"
                        + "            part\n"
                        + "            where\n"
                        + "            p_name like 'drab%'\n"
                        + "          )\n"
                        + "          and ps_availqty > (\n"
                        + "            select\n"
                        + "            0.5 * sum(l_quantity)\n"
                        + "            from\n"
                        + "            lineitem\n"
                        + "            where\n"
                        + "            l_partkey = ps_partkey\n"
                        + "            and l_suppkey = ps_suppkey\n"
                        + "            and l_shipdate >= date '1996-01-01'\n"
                        + "            and l_shipdate < date '1996-01-01' + interval '1' year\n"
                        + "          )\n"
                        + "        )\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_name = 'KENYA';";
        String Q19 =
                "select\n"
                        + "        sum(l_extendedprice* (1 - l_discount)) as revenue\n"
                        + "from\n"
                        + "        lineitem,\n"
                        + "        part\n"
                        + "where\n"
                        + "        (\n"
                        + "          p_partkey = l_partkey\n"
                        + "          and p_brand = 'Brand#52'\n"
                        + "          and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                        + "          and l_quantity >= 3 and l_quantity <= 3 + 10\n"
                        + "          and p_size between 1 and 5\n"
                        + "          and l_shipmode in ('AIR', 'AIR REG')\n"
                        + "          and l_shipinstruct = 'DELIVER IN PERSON'\n"
                        + "        )\n"
                        + "        or\n"
                        + "        (\n"
                        + "          p_partkey = l_partkey\n"
                        + "          and p_brand = 'Brand#43'\n"
                        + "          and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                        + "          and l_quantity >= 12 and l_quantity <= 12 + 10\n"
                        + "          and p_size between 1 and 10\n"
                        + "          and l_shipmode in ('AIR', 'AIR REG')\n"
                        + "          and l_shipinstruct = 'DELIVER IN PERSON'\n"
                        + "        )\n"
                        + "        or\n"
                        + "        (\n"
                        + "          p_partkey = l_partkey\n"
                        + "          and p_brand = 'Brand#52'\n"
                        + "          and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                        + "          and l_quantity >= 21 and l_quantity <= 21 + 10\n"
                        + "          and p_size between 1 and 15\n"
                        + "          and l_shipmode in ('AIR', 'AIR REG')\n"
                        + "          and l_shipinstruct = 'DELIVER IN PERSON'\n"
                        + "        );";
        String Q18 =
                "select\n"
                        + "        c_name,\n"
                        + "        c_custkey,\n"
                        + "        o_orderkey,\n"
                        + "        o_orderdate,\n"
                        + "        o_totalprice,\n"
                        + "        sum(l_quantity)\n"
                        + "from\n"
                        + "        customer,\n"
                        + "        orders,\n"
                        + "        lineitem\n"
                        + "where\n"
                        + "        o_orderkey in (\n"
                        + "                select\n"
                        + "                        l_orderkey\n"
                        + "                from\n"
                        + "                        lineitem\n"
                        + "                group by\n"
                        + "                        l_orderkey having\n"
                        + "                                sum(l_quantity) > 312\n"
                        + "        )\n"
                        + "        and c_custkey = o_custkey\n"
                        + "        and o_orderkey = l_orderkey\n"
                        + "group by\n"
                        + "        c_name,\n"
                        + "        c_custkey,\n"
                        + "        o_orderkey,\n"
                        + "        o_orderdate,\n"
                        + "        o_totalprice;";
        String Q17 =
                "select\n"
                        + "        sum(l_extendedprice) / 7.0 as avg_yearly\n"
                        + "from\n"
                        + "        lineitem,\n"
                        + "        part\n"
                        + "where\n"
                        + "        p_partkey = l_partkey\n"
                        + "        and p_brand = 'Brand#51'\n"
                        + "        and p_container = 'WRAP PACK'\n"
                        + "        and l_quantity < (\n"
                        + "                select\n"
                        + "                        0.2 * avg(l_quantity)\n"
                        + "                from\n"
                        + "                        lineitem\n"
                        + "                where\n"
                        + "                        l_partkey = p_partkey\n"
                        + "        );";
        String Q16 =
                "select\n"
                        + "        p_brand,\n"
                        + "        p_type,\n"
                        + "        p_size,\n"
                        + "        count(distinct ps_suppkey) as supplier_cnt\n"
                        + "from\n"
                        + "        partsupp,\n"
                        + "        part\n"
                        + "where\n"
                        + "        p_partkey = ps_partkey\n"
                        + "        and p_brand <> 'Brand#45'\n"
                        + "        and p_type not like 'SMALL ANODIZED%'\n"
                        + "        and p_size in (47, 15, 37, 30, 46, 16, 18, 6)\n"
                        + "        and ps_suppkey not in (\n"
                        + "                select\n"
                        + "                        s_suppkey\n"
                        + "                from\n"
                        + "                        supplier\n"
                        + "                where\n"
                        + "                        s_comment like '%Customer%Complaints%'\n"
                        + "        )\n"
                        + "group by\n"
                        + "        p_brand,\n"
                        + "        p_type,\n"
                        + "        p_size;";
        String Q15 =
                "with revenue0(supplier_no, total_revenue)  as\n"
                        + "    (\n"
                        + "    select\n"
                        + "        l_suppkey,\n"
                        + "        sum(l_extendedprice * (1 - l_discount))\n"
                        + "    from\n"
                        + "        lineitem\n"
                        + "    where\n"
                        + "        l_shipdate >= date '1995-02-01'\n"
                        + "        and l_shipdate < date '1995-02-01' + interval '3' month\n"
                        + "    group by\n"
                        + "        l_suppkey\n"
                        + "    )\n"
                        + "select\n"
                        + "    s_suppkey,\n"
                        + "    s_name,\n"
                        + "    s_address,\n"
                        + "    s_phone,\n"
                        + "    total_revenue\n"
                        + "from\n"
                        + "    supplier,\n"
                        + "    revenue0\n"
                        + "where\n"
                        + "    s_suppkey = supplier_no\n"
                        + "    and total_revenue = (\n"
                        + "        select\n"
                        + "            max(total_revenue)\n"
                        + "        from\n"
                        + "            revenue0\n"
                        + "    );";
        String Q14 =
                "select\n"
                        + "        100.00 * sum(case\n"
                        + "                when p_type like 'PROMO%'\n"
                        + "                        then l_extendedprice * (1 - l_discount)\n"
                        + "                else 0\n"
                        + "        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n"
                        + "from\n"
                        + "        lineitem,\n"
                        + "        part\n"
                        + "where\n"
                        + "        l_partkey = p_partkey\n"
                        + "        and l_shipdate >= date '1997-06-01'\n"
                        + "        and l_shipdate < date '1997-06-01' + interval '1' month;";
        String Q13 =
                "select\n"
                        + "        c_count,\n"
                        + "        count(*) as custdist\n"
                        + "from\n"
                        + "        (\n"
                        + "                select\n"
                        + "                        c_custkey,\n"
                        + "                        count(o_orderkey) as c_count\n"
                        + "                from\n"
                        + "                        customer left outer join orders on\n"
                        + "                                c_custkey = o_custkey\n"
                        + "                                and o_comment not like '%special%deposits%'\n"
                        + "                group by\n"
                        + "                        c_custkey\n"
                        + "        ) c_orders\n"
                        + "group by\n"
                        + "        c_count;";
        String Q12 =
                "select\n"
                        + "        l_shipmode,\n"
                        + "        sum(case\n"
                        + "                when o_orderpriority = '1-URGENT'\n"
                        + "                        or o_orderpriority = '2-HIGH'\n"
                        + "                        then 1\n"
                        + "                else 0\n"
                        + "        end) as high_line_count,\n"
                        + "        sum(case\n"
                        + "                when o_orderpriority <> '1-URGENT'\n"
                        + "                        and o_orderpriority <> '2-HIGH'\n"
                        + "                        then 1\n"
                        + "                else 0\n"
                        + "        end) as low_line_count\n"
                        + "from\n"
                        + "        orders,\n"
                        + "        lineitem\n"
                        + "where\n"
                        + "        o_orderkey = l_orderkey\n"
                        + "        and l_shipmode in ('FOB', 'AIR')\n"
                        + "        and l_commitdate < l_receiptdate\n"
                        + "        and l_shipdate < l_commitdate\n"
                        + "        and l_receiptdate >= date '1997-01-01'\n"
                        + "        and l_receiptdate < date '1997-01-01' + interval '1' year\n"
                        + "group by\n"
                        + "        l_shipmode;";
        String Q11 =
                "select\n"
                        + "        ps_partkey,\n"
                        + "        sum(ps_supplycost * ps_availqty) as value11\n"
                        + "from\n"
                        + "        partsupp,\n"
                        + "        supplier,\n"
                        + "        nation\n"
                        + "where\n"
                        + "        ps_suppkey = s_suppkey\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_name = 'EGYPT'\n"
                        + "group by\n"
                        + "        ps_partkey having\n"
                        + "                sum(ps_supplycost * ps_availqty) > (\n"
                        + "                        select\n"
                        + "                                sum(ps_supplycost * ps_availqty) * 0.0001000000\n"
                        + "                        from\n"
                        + "                                partsupp,\n"
                        + "                                supplier,\n"
                        + "                                nation\n"
                        + "                        where\n"
                        + "                                ps_suppkey = s_suppkey\n"
                        + "                                and s_nationkey = n_nationkey\n"
                        + "                                and n_name = 'EGYPT'\n"
                        + "                );";
        String Q10 =
                "select\n"
                        + "        c_custkey,\n"
                        + "        c_name,\n"
                        + "        sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
                        + "        c_acctbal,\n"
                        + "        n_name,\n"
                        + "        c_address,\n"
                        + "        c_phone,\n"
                        + "        c_comment\n"
                        + "from\n"
                        + "        customer,\n"
                        + "        orders,\n"
                        + "        lineitem,\n"
                        + "        nation\n"
                        + "where\n"
                        + "        c_custkey = o_custkey\n"
                        + "        and l_orderkey = o_orderkey\n"
                        + "        and o_orderdate >= date '1993-02-01'\n"
                        + "        and o_orderdate < date '1993-02-01' + interval '3' month\n"
                        + "        and l_returnflag = 'R'\n"
                        + "        and c_nationkey = n_nationkey\n"
                        + "group by\n"
                        + "        c_custkey,\n"
                        + "        c_name,\n"
                        + "        c_acctbal,\n"
                        + "        c_phone,\n"
                        + "        n_name,\n"
                        + "        c_address,\n"
                        + "        c_comment;";
        String Q9 =
                "select\n"
                        + "        nation,\n"
                        + "        o_year,\n"
                        + "        sum(amount) as sum_profit\n"
                        + "from\n"
                        + "        (\n"
                        + "                select\n"
                        + "                        n_name as nation,\n"
                        + "                        extract(year from o_orderdate) as o_year,\n"
                        + "                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
                        + "                from\n"
                        + "                        part,\n"
                        + "                        supplier,\n"
                        + "                        lineitem,\n"
                        + "                        partsupp,\n"
                        + "                        orders,\n"
                        + "                        nation\n"
                        + "                where\n"
                        + "                        s_suppkey = l_suppkey\n"
                        + "                        and ps_suppkey = l_suppkey\n"
                        + "                        and ps_partkey = l_partkey\n"
                        + "                        and p_partkey = l_partkey\n"
                        + "                        and o_orderkey = l_orderkey\n"
                        + "                        and s_nationkey = n_nationkey\n"
                        + "                        and p_name like '%maroon%'\n"
                        + "        ) as profit\n"
                        + "group by\n"
                        + "        nation,\n"
                        + "        o_year;";
        String Q8 =
                "select\n"
                        + "        o_year,\n"
                        + "        sum(case\n"
                        + "                when nation = 'BRAZIL' then volume\n"
                        + "                else 0\n"
                        + "        end) / sum(volume) as mkt_share\n"
                        + "from\n"
                        + "        (\n"
                        + "                select\n"
                        + "                        extract(year from o_orderdate) as o_year,\n"
                        + "                        l_extendedprice * (1 - l_discount) as volume,\n"
                        + "                        n2.n_name as nation\n"
                        + "                from\n"
                        + "                        part,\n"
                        + "                        supplier,\n"
                        + "                        lineitem,\n"
                        + "                        orders,\n"
                        + "                        customer,\n"
                        + "                        nation n1,\n"
                        + "                        nation n2,\n"
                        + "                        region\n"
                        + "                where\n"
                        + "                        p_partkey = l_partkey\n"
                        + "                        and s_suppkey = l_suppkey\n"
                        + "                        and l_orderkey = o_orderkey\n"
                        + "                        and o_custkey = c_custkey\n"
                        + "                        and c_nationkey = n1.n_nationkey\n"
                        + "                        and n1.n_regionkey = r_regionkey\n"
                        + "                        and r_name = 'AMERICA'\n"
                        + "                        and s_nationkey = n2.n_nationkey\n"
                        + "                        and o_orderdate between date '1995-01-01' and date '1996-12-31'\n"
                        + "                        and p_type = 'LARGE ANODIZED COPPER'\n"
                        + "        ) as all_nations\n"
                        + "group by\n"
                        + "        o_year;";
        String Q7 =
                "select\n"
                        + "        supp_nation,\n"
                        + "        cust_nation,\n"
                        + "        l_year,\n"
                        + "        sum(volume) as revenue\n"
                        + "from\n"
                        + "        (\n"
                        + "                select\n"
                        + "                        n1.n_name as supp_nation,\n"
                        + "                        n2.n_name as cust_nation,\n"
                        + "                        extract(year from l_shipdate) as l_year,\n"
                        + "                        l_extendedprice * (1 - l_discount) as volume\n"
                        + "                from\n"
                        + "                        supplier,\n"
                        + "                        lineitem,\n"
                        + "                        orders,\n"
                        + "                        customer,\n"
                        + "                        nation n1,\n"
                        + "                        nation n2\n"
                        + "                where\n"
                        + "                        s_suppkey = l_suppkey\n"
                        + "                        and o_orderkey = l_orderkey\n"
                        + "                        and c_custkey = o_custkey\n"
                        + "                        and s_nationkey = n1.n_nationkey\n"
                        + "                        and c_nationkey = n2.n_nationkey\n"
                        + "                        and (\n"
                        + "                                (n1.n_name = 'CANADA' and n2.n_name = 'BRAZIL')\n"
                        + "                                or (n1.n_name = 'BRAZIL' and n2.n_name = 'CANADA')\n"
                        + "                        )\n"
                        + "                        and l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
                        + "        ) as shipping\n"
                        + "group by\n"
                        + "        supp_nation,\n"
                        + "        cust_nation,\n"
                        + "        l_year;";
        String Q6 =
                "select\n"
                        + "        sum(l_extendedprice * l_discount) as revenue\n"
                        + "from\n"
                        + "        lineitem\n"
                        + "where\n"
                        + "        l_shipdate >= date '1996-01-01'\n"
                        + "        and l_shipdate < date '1996-01-01' + interval '1' year\n"
                        + "        and l_discount between 0.02 - 0.01 and 0.02 + 0.01\n"
                        + "        and l_quantity < 24;";
        String Q5 =
                "select\n"
                        + "        n_name,\n"
                        + "        sum(l_extendedprice * (1 - l_discount)) as revenue\n"
                        + "from\n"
                        + "        customer,\n"
                        + "        orders,\n"
                        + "        lineitem,\n"
                        + "        supplier,\n"
                        + "        nation,\n"
                        + "        region\n"
                        + "where\n"
                        + "        c_custkey = o_custkey\n"
                        + "        and l_orderkey = o_orderkey\n"
                        + "        and l_suppkey = s_suppkey\n"
                        + "        and c_nationkey = s_nationkey\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_regionkey = r_regionkey\n"
                        + "        and r_name = 'EUROPE'\n"
                        + "        and o_orderdate >= date '1996-01-01'\n"
                        + "        and o_orderdate < date '1996-01-01' + interval '1' year\n"
                        + "group by\n"
                        + "        n_name;";
        String Q4 =
                "select\n"
                        + "        o_orderpriority,\n"
                        + "        count(*) as order_count\n"
                        + "from\n"
                        + "        orders\n"
                        + "where\n"
                        + "        o_orderdate >= date '1996-07-01'\n"
                        + "        and o_orderdate < date '1996-07-01' + interval '3' month\n"
                        + "        and exists (\n"
                        + "          select\n"
                        + "          *\n"
                        + "          from\n"
                        + "          lineitem\n"
                        + "          where\n"
                        + "          l_orderkey = o_orderkey\n"
                        + "          and l_commitdate < l_receiptdate\n"
                        + "        )\n"
                        + "group by\n"
                        + "        o_orderpriority;";
        String Q3 =
                "select\n"
                        + "        l_orderkey,\n"
                        + "        sum(l_extendedprice * (1 - l_discount)) as revenue,\n"
                        + "        o_orderdate,\n"
                        + "        o_shippriority\n"
                        + "from\n"
                        + "        customer,\n"
                        + "        orders,\n"
                        + "        lineitem\n"
                        + "where\n"
                        + "        c_mktsegment = 'MACHINERY'\n"
                        + "        and c_custkey = o_custkey\n"
                        + "        and l_orderkey = o_orderkey\n"
                        + "        and o_orderdate < date '1995-03-23'\n"
                        + "        and l_shipdate > date '1995-03-23'\n"
                        + "group by\n"
                        + "        l_orderkey,\n"
                        + "        o_orderdate,\n"
                        + "        o_shippriority;";
        String Q2 =
                "select\n"
                        + "        s_acctbal,\n"
                        + "        s_name,\n"
                        + "        n_name,\n"
                        + "        p_partkey,\n"
                        + "        p_mfgr,\n"
                        + "        s_address,\n"
                        + "        s_phone,\n"
                        + "        s_comment\n"
                        + "from\n"
                        + "        part,\n"
                        + "        supplier,\n"
                        + "        partsupp,\n"
                        + "        nation,\n"
                        + "        region\n"
                        + "where\n"
                        + "        p_partkey = ps_partkey\n"
                        + "        and s_suppkey = ps_suppkey\n"
                        + "        and p_size = 48\n"
                        + "        and p_type like '%STEEL'\n"
                        + "        and s_nationkey = n_nationkey\n"
                        + "        and n_regionkey = r_regionkey\n"
                        + "        and r_name = 'EUROPE'\n"
                        + "        and ps_supplycost = (\n"
                        + "          select\n"
                        + "          min(ps_supplycost)\n"
                        + "          from\n"
                        + "          partsupp,\n"
                        + "          supplier,\n"
                        + "          nation,\n"
                        + "          region\n"
                        + "          where\n"
                        + "          p_partkey = ps_partkey\n"
                        + "          and s_suppkey = ps_suppkey\n"
                        + "          and s_nationkey = n_nationkey\n"
                        + "          and n_regionkey = r_regionkey\n"
                        + "          and r_name = 'EUROPE'\n"
                        + "        )";
        String Q1 =
                "select\n"
                        + "        l_returnflag,\n"
                        + "        l_linestatus,\n"
                        + "        sum(l_quantity) as sum_qty,\n"
                        + "        sum(l_extendedprice) as sum_base_price,\n"
                        + "        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
                        + "        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
                        + "        avg(l_quantity) as avg_qty,\n"
                        + "        avg(l_extendedprice) as avg_price,\n"
                        + "        avg(l_discount) as avg_disc,\n"
                        + "        count(*) as count_order\n"
                        + "from\n"
                        + "        lineitem\n"
                        + "where\n"
                        + "        l_shipdate <= date '1998-12-01' - interval '90' day\n"
                        + "group by\n"
                        + "        l_returnflag,\n"
                        + "        l_linestatus;";

        map.put("Q22", Q22);
        map.put("Q21", Q21);
        map.put("Q20", Q20);
        map.put("Q19", Q19);
        map.put("Q18", Q18);
        map.put("Q17", Q17);
        map.put("Q16", Q16);
        map.put("Q15", Q15);
        map.put("Q14", Q14);
        map.put("Q13", Q13);
        map.put("Q12", Q12);
        map.put("Q11", Q11);
        map.put("Q10", Q10);
        map.put("Q9", Q9);
        map.put("Q8", Q8);
        map.put("Q7", Q7);
        map.put("Q6", Q6);
        map.put("Q5", Q5);
        map.put("Q4", Q4);
        map.put("Q3", Q3);
        map.put("Q2", Q2);
        map.put("Q1", Q1);

        return map;
    }

    public HashMap<String, String> getSinkTables() {
        HashMap<String, String> map = new HashMap<>();
        String sinkTableQ1 =
                "CREATE TABLE sink_table_q1 (\n"
                        + "    l_returnflag CHAR(1),\n"
                        + "    l_linestatus CHAR(1),\n"
                        + "    sum_qty DECIMAL(38, 2),\n"
                        + "    sum_base_price DECIMAL(38, 2),\n"
                        + "    sum_disc_price DECIMAL(38, 4),\n"
                        + "    sum_charge DECIMAL(38, 6),\n"
                        + "    avg_qty DECIMAL(38, 6),\n"
                        + "    avg_price DECIMAL(38, 6),\n"
                        + "    avg_disc DECIMAL(38, 6),\n"
                        + "    count_order BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ2 =
                "CREATE TABLE sink_table_q2 (\n"
                        + "    s_acctbal DECIMAL(38, 2),\n"
                        + "    s_name STRING,\n"
                        + "    n_name STRING,\n"
                        + "    p_partkey BIGINT,\n"
                        + "    p_mfgr STRING,\n"
                        + "    s_address STRING,\n"
                        + "    s_phone STRING,\n"
                        + "    s_comment STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ3 =
                "CREATE TABLE sink_table_q3 (\n"
                        + "    l_orderkey BIGINT,\n"
                        + "    revenue DECIMAL(38, 2),\n"
                        + "    o_orderdate DATE,\n"
                        + "    o_shippriority INT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ4 =
                "CREATE TABLE sink_table_q4 (\n"
                        + "    o_orderpriority CHAR(15) ,\n"
                        + "    order_count BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ5 =
                "CREATE TABLE sink_table_q5 (\n"
                        + "n_name CHAR(25), "
                        + "revenue DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ6 =
                "CREATE TABLE sink_table_q6 (\n"
                        + "revenue DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ7 =
                "CREATE TABLE sink_table_q7 (\n"
                        + "supp_nation CHAR(25), "
                        + "cust_nation CHAR(25), "
                        + "l_year BIGINT, "
                        + "revenue DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ8 =
                "CREATE TABLE sink_table_q8 (\n"
                        + "o_year BIGINT, "
                        + "mkt_share DECIMAL(38, 6)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ9 =
                "CREATE TABLE sink_table_q9 (\n"
                        + "nation CHAR(25), "
                        + "o_year BIGINT, "
                        + "sum_profit DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ10 =
                "CREATE TABLE sink_table_q10 (\n"
                        + "    c_custkey BIGINT,\n"
                        + "    c_name STRING,\n"
                        + "    revenue DECIMAL(38, 2),\n"
                        + "    c_acctbal DECIMAL(38, 2),\n"
                        + "    n_name STRING,\n"
                        + "    c_address STRING,\n"
                        + "    c_phone STRING,\n"
                        + "    c_comment STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ11 =
                "CREATE TABLE sink_table_q11 (\n"
                        + "ps_partkey INT, "
                        + "value11 DECIMAL(38, 2)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ12 =
                "CREATE TABLE sink_table_q12 (\n"
                        + "    l_shipmode STRING,\n"
                        + "    high_line_count BIGINT,\n"
                        + "    low_line_count BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ13 =
                "CREATE TABLE sink_table_q13 (\n"
                        + "    c_custkey BIGINT,\n"
                        + "    c_count BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ14 =
                "CREATE TABLE sink_table_q14 (\n"
                        + "promo_revenue DECIMAL(38, 6)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ15 =
                "CREATE TABLE sink_table_q15 (\n"
                        + "s_suppkey INT, "
                        + "s_name CHAR(25), "
                        + "s_address VARCHAR(40), "
                        + "s_phone CHAR(15), "
                        + "total_revenue DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ16 =
                "CREATE TABLE sink_table_q16 (\n"
                        + "    p_brand STRING,\n"
                        + "    p_type STRING,\n"
                        + "    p_size INT,\n"
                        + "    supplier_cnt BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ17 =
                "CREATE TABLE sink_table_q17 (\n"
                        + "avg_yearly DECIMAL(38, 6)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ18 =
                "CREATE TABLE sink_table_q18 (\n"
                        + "c_name VARCHAR(25), "
                        + "c_custkey INT, "
                        + "o_orderkey INT, "
                        + "o_orderdate DATE, "
                        + "o_totalprice DECIMAL(15, 2), "
                        + "sum_lq DECIMAL(38, 2)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ19 =
                "CREATE TABLE sink_table_q19 (\n"
                        + "revenue DECIMAL(38, 4)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ20 =
                "CREATE TABLE sink_table_q20 (\n"
                        + "s_name CHAR(25), "
                        + "s_address VARCHAR(40)"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ21 =
                "CREATE TABLE sink_table_q21 (\n"
                        + "    s_name STRING,\n"
                        + "    count_order BIGINT\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        String sinkTableQ22 =
                "CREATE TABLE sink_table_q22 (\n"
                        + "    cntrycode STRING,\n"
                        + "    numcust BIGINT,\n"
                        + "    totacctbal DECIMAL(38, 2)\n"
                        + ") WITH (\n"
                        + "    'connector' = 'blackhole'\n"
                        + ");";
        map.put("Q1", sinkTableQ1);
        map.put("Q2", sinkTableQ2);
        map.put("Q3", sinkTableQ3);
        map.put("Q4", sinkTableQ4);
        map.put("Q5", sinkTableQ5);
        map.put("Q6", sinkTableQ6);
        map.put("Q7", sinkTableQ7);
        map.put("Q8", sinkTableQ8);
        map.put("Q9", sinkTableQ9);
        map.put("Q10", sinkTableQ10);
        map.put("Q11", sinkTableQ11);
        map.put("Q12", sinkTableQ12);
        map.put("Q13", sinkTableQ13);
        map.put("Q14", sinkTableQ14);
        map.put("Q15", sinkTableQ15);
        map.put("Q16", sinkTableQ16);
        map.put("Q17", sinkTableQ17);
        map.put("Q18", sinkTableQ18);
        map.put("Q19", sinkTableQ19);
        map.put("Q20", sinkTableQ20);
        map.put("Q21", sinkTableQ21);
        map.put("Q22", sinkTableQ22);
        return map;
    }
}
