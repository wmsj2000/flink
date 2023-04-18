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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A calculator that calculate parallelism and resource from a graph of {@link Transformation}.
 *
 * @author Quentin Qiu
 */
public class ParallelismAndResourceOfMultipleInputJoin {

    /** Original input transformations for {@link MultipleInputStreamJoinOperator}. */
    private final List<Transformation<?>> inputTransforms;

    /**
     * The tail (root) transformation of the transformation-graph in {@link
     * MultipleInputStreamJoinOperator}.
     */
    private final Transformation<?> tailTransform;
    /** record the visited transformations */
    private final List<Transformation<?>> visitedTransforms;

    private int parallelism;
    private int maxParallelism;
    private ResourceSpec minResources;
    private ResourceSpec preferredResources;

    /** Managed memory weight for batch operator in mebibyte. */
    private int managedMemoryWeight;

    public ParallelismAndResourceOfMultipleInputJoin(
            List<Transformation<?>> inputTransforms, Transformation<?> tailTransform) {
        this.inputTransforms = inputTransforms;
        this.tailTransform = tailTransform;
        this.visitedTransforms = new ArrayList<>();
        this.parallelism = -1;
        this.maxParallelism = -1;
    }

    public void calculate() {
        visit(tailTransform);
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public ResourceSpec getMinResources() {
        return minResources;
    }

    public ResourceSpec getPreferredResources() {
        return preferredResources;
    }

    public int getManagedMemoryWeight() {
        return managedMemoryWeight;
    }

    private void visit(Transformation<?> transform) {
        // ignore UnionTransformation because it's not a real operator
        if (!(transform instanceof UnionTransformation
                || transform instanceof PartitionTransformation)) {
            calcParallelismAndResource(transform);
        }
        if (!visitedTransforms.contains(transform)) {
            visitTransformation(transform);
        }
    }

    private void calcParallelismAndResource(Transformation<?> transform) {
        // do not check the parallelisms in multiple-input node are same,
        // because we should consider the following case:
        // Source1(100 parallelism) -> Calc(100 parallelism) -\
        //                                                     -> union -> join -> ...
        // Source2(50 parallelism)  -> Calc(50 parallelism) -/
        parallelism = Math.max(parallelism, transform.getParallelism());

        int currentMaxParallelism = transform.getMaxParallelism();
        if (maxParallelism < 0) {
            maxParallelism = currentMaxParallelism;
        } else {
            checkState(
                    currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
                    "Max parallelism of a transformation in MultipleInput node is different from others. This is a bug.");
        }

        if (minResources == null) {
            minResources = transform.getMinResources();
            preferredResources = transform.getPreferredResources();
            managedMemoryWeight =
                    transform
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        } else {
            minResources = minResources.merge(transform.getMinResources());
            preferredResources = preferredResources.merge(transform.getPreferredResources());
            managedMemoryWeight +=
                    transform
                            .getManagedMemoryOperatorScopeUseCaseWeights()
                            .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void visitTransformation(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            visitOneInputTransformation((OneInputTransformation) transform);
        } else if (transform instanceof TwoInputTransformation) {
            visitTwoInputTransformation((TwoInputTransformation) transform);
        } else if (transform instanceof PartitionTransformation) {
            visitPartitionTransformation((PartitionTransformation<RowData>) transform);
        } else {
            throw new RuntimeException("Unsupported Transformation: " + transform);
        }
    }

    private void visitPartitionTransformation(PartitionTransformation<RowData> transform) {
        visitedTransforms.add(transform);
        Transformation<?> input = transform.getInputs().get(0);
        if (!inputTransforms.contains(transform)) {
            visit(input);
        }
    }

    private void visitOneInputTransformation(OneInputTransformation<RowData, RowData> transform) {
        visitedTransforms.add(transform);
        Transformation<?> input = transform.getInputs().get(0);
        if (!inputTransforms.contains(transform)) {
            visit(input);
        }
    }

    private void visitTwoInputTransformation(
            TwoInputTransformation<RowData, RowData, RowData> transform) {
        visitedTransforms.add(transform);
        Transformation<?> input1 = transform.getInput1();
        Transformation<?> input2 = transform.getInput2();
        if (!inputTransforms.contains(input1)) {
            visit(input1);
        }
        if (!inputTransforms.contains(input2)) {
            visit(input2);
        }
    }
}
