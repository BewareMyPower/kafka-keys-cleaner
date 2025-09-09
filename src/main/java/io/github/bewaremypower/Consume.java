/**
 * Copyright 2025 Yunze Xu
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
package io.github.bewaremypower;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;
import static picocli.CommandLine.ParentCommand;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

@Command(name = "consume", description = "Produces messages.")
@Slf4j
public class Consume implements Callable<Integer> {

  @ParentCommand App app;

  @Parameters(index = "0", description = "group id")
  String groupId;

  @Override
  public Integer call() throws Exception {
    @Cleanup final var consumer = app.newConsumer(groupId);
    consumer.subscribe(
        List.of(app.getTopic()),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            log.info("{} is assigned with {}", groupId, collection);
          }
        });
    var lastReceived = System.currentTimeMillis();
    while (System.currentTimeMillis() - lastReceived < 6000) {
      final var records = consumer.poll(Duration.ofMillis(100));
      records.forEach(
          r ->
              log.info(
                  "Received {} => {} from {}-{}@{} timestamp: {}",
                  r.key(),
                  r.value(),
                  r.topic(),
                  r.partition(),
                  r.offset(),
                  r.timestamp()));
      if (!records.isEmpty()) {
        lastReceived = System.currentTimeMillis();
      }
    }
    return 0;
  }
}
