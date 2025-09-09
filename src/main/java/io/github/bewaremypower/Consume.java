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

import java.util.concurrent.Callable;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Command(name = "consume", description = "Produces messages.")
@Slf4j
public class Consume implements Callable<Integer> {

  @ParentCommand App app;

  @Parameters(index = "0", description = "group id")
  String groupId;

  @Override
  public Integer call() throws Exception {
    @Cleanup final var consumer = app.newConsumer(groupId);
    KafkaUtils.replayTopic(
        consumer,
        app.getTopic(),
        r -> {
          log.info(
              "Received {} => {} from {}-{}@{} timestamp: {}",
              r.key(),
              r.value(),
              r.topic(),
              r.partition(),
              r.offset(),
              r.timestamp());
        });
    return 0;
  }
}
