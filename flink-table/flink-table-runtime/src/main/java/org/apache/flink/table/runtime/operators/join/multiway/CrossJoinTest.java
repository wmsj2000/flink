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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/** a test class for crossJoin() in MultipleInputStreamJoinOperator */
public class CrossJoinTest {
    public static List<List<String>> crossJoin(List<List<String>> lists) {
        // 如果输入的列表为空，则返回空列表
        if (lists == null || lists.isEmpty()) {
            return new ArrayList<>();
        }
        // 如果输入的列表只有一个，则将其每个元素放入一个列表中，作为结果返回
        if (lists.size() == 1) {
            List<List<String>> result = new ArrayList<>();
            for (String s : lists.get(0)) {
                result.add(Arrays.asList(s));
            }
            return result;
        }
        // 取出第一个列表，并对其余列表进行递归调用
        List<String> firstList = lists.get(0);
        List<List<String>> subLists = crossJoin(lists.subList(1, lists.size()));
        // 将第一个列表中的每个元素依次与递归调用得到的列表中的元素进行组合
        List<List<String>> result = new ArrayList<>();
        for (String s : firstList) {
            for (List<String> subList : subLists) {
                List<String> newList = new ArrayList<>();
                newList.add(s);
                newList.addAll(subList);
                result.add(newList);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        List<List<String>> input = new ArrayList<>();
        input.add(Arrays.asList("q", "w", "e"));
        input.add(Arrays.asList("a", "s"));
        input.add(Arrays.asList("d", "dq", "fwfw"));
        List<List<String>> output = crossJoin(input);
        for (List<String> list : output) {
            System.out.println(list);
        }
        List<Integer> selectivities = new ArrayList<>();
        Collections.addAll(selectivities, 3, 1, 2, 4, 0);
        int[] sortedIndexArr =
                IntStream.range(0, selectivities.size())
                        .boxed()
                        .sorted(Comparator.comparing(selectivities::get))
                        .mapToInt(Integer::intValue)
                        .toArray();
        System.out.println(Arrays.toString(sortedIndexArr));
    }
}
