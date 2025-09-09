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
import static picocli.CommandLine.Option;
import static picocli.CommandLine.ParentCommand;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

@Command(name = "produce", description = "Produces messages.")
@Slf4j
public class Produce implements Callable<Integer> {

  @ParentCommand private App app;

  @Option(
      names = {"-f"},
      required = true,
      description =
          "The file that includes all key-values to send in order, each line whose format is \"key => value\" represents the message whose key is \"key\" and value is \"value\"")
  private File file;

  @Override
  public Integer call() throws Exception {
    @Cleanup final var producer = app.newProducer();
    @Cleanup final var stream = Files.lines(file.toPath());
    stream.forEach(
        s -> {
          final var tokens = s.split("=>");
          if (tokens.length != 2) {
            return;
          }
          final var key = tokens[0].trim();
          final var value = tokens[1].trim();
          try {
            final var metadata =
                producer.send(new ProducerRecord<>(app.getTopic(), null, key, value)).get();
            log.info(
                "Sent {} => {} to {}-{}@{} timestamp: {}",
                key,
                value,
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp());
          } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send {} => {}", key, value, e);
          }
        });
    return 0;
  }
}
