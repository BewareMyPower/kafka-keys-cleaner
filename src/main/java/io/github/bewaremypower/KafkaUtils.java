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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class KafkaUtils {

  public static void replayTopic(
      KafkaConsumer<String, String> consumer,
      String topic,
      Consumer<ConsumerRecord<String, String>> recordConsumer) {
    consumer.subscribe(
        List.of(topic),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            log.info("Assigned with {}", collection);
          }
        });
    final var tp = new TopicPartition(topic, 0);
    final var endOffset = consumer.endOffsets(List.of(tp)).get(tp);
    if (endOffset == null) {
      log.warn("No end offset found for consumer");
      return;
    }
    long offset = -1L;
    while (offset + 1 < endOffset) {
      final var records = consumer.poll(Duration.ofMillis(100));
      for (final var record : records) {
        recordConsumer.accept(record);
        offset = record.offset();
      }
    }
  }
}
