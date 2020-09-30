/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.mantis.samples;

import com.netflix.mantis.samples.source.RandomRequestSource;
import com.netflix.mantis.samples.stage.AggregationStage;
import com.netflix.mantis.samples.stage.CollectStage;
import com.netflix.mantis.samples.stage.GroupByStage;
import io.mantisrx.runtime.Job;
import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.MantisJobProvider;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.executor.LocalJobExecutorNetworked;
import io.mantisrx.runtime.sink.Sinks;
import lombok.extern.slf4j.Slf4j;


/**
 * This sample demonstrates the use of a multi-stage job in Mantis. Multi-stage jobs are useful when a single
 * container is incapable of processing the entire stream of events.
 * Each stage represents one of these types of
 * computations Scalar->Scalar, Scalar->Group, Group->Scalar, Group->Group.
 *
 * At deploy time the user can configure the number workers for each stage and the resource requirements for each worker.
 * This sample has 3 stages
 * 1. {@link GroupByStage} Receives the raw events, groups them by their category and sends it to the workers of stage 2 in such a way
 * that all events for a particular group will land on the exact same worker of stage 2.
 * 2. {@link AggregationStage} Receives events tagged by their group from the previous stage. Windows over them and
 * sums up the counts of each group it has seen.
 * 3. {@link CollectStage} Recieves the aggregates generated by the previous stage, collects them over a window and
 * generates a consolidated report which is sent to the default Server Sent Event (SSE) sink.
 *
 * Run this sample by executing the main method of this class. Then look for the SSE port where the output of this job
 * will be available for streaming. E.g  Serving modern HTTP SSE server sink on port: 8299
 * via command line do ../gradlew execute
 */

@Slf4j
public class RequestAggregationJob extends MantisJobProvider<String> {

    @Override
    public Job<String> getJobInstance() {

        return MantisJob
                // Stream Request Events from our random data generator source
                .source(new RandomRequestSource())

                // Groups requests by path
                .stage(new GroupByStage(), GroupByStage.config())

                // Computes count per path over a window
                .stage(new AggregationStage(), AggregationStage.config())

                // Collects the data and makes it availabe over SSE
                .stage(new CollectStage(), CollectStage.config())

                // Reuse built in sink that eagerly subscribes and delivers data over SSE
                .sink(Sinks.sysout())

                .metadata(new Metadata.Builder()
                        .name("GroupByPath")
                        .description("Connects to a random data generator source"
                                + " and counts the number of requests for each uri within a window")
                        .build())
                .create();

    }

    public static void main(String[] args) {
        // To run locally we use the LocalJobExecutor
        LocalJobExecutorNetworked.execute(new RequestAggregationJob().getJobInstance());
    }
}