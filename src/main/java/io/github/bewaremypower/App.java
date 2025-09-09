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
import static picocli.CommandLine.Parameters;

import java.util.Properties;
import java.util.concurrent.Callable;
import lombok.Getter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

@Command(
    name = "config",
    subcommands = {Produce.class, Compaction.class, Consume.class},
    description = "Run operations on a compacted topic")
public class App implements Callable<Integer> {

  @Parameters(index = "0", description = "The Kafka bootstrap servers")
  private String bootstrapServers;

  @Getter
  @Parameters(index = "", description = "The Kafka topic")
  private String topic;

  @Option(
      names = {"--token"},
      description = "authentication token (mechanism: SASL_SSL)")
  private String token;

  @Override
  public Integer call() {
    return 0;
  }

  public KafkaProducer<String, String> newProducer() {
    final var properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    if (token != null) {
      configureSasl(properties, token);
    }
    return new KafkaProducer<>(properties);
  }

  public KafkaConsumer<String, String> newConsumer(String groupId) {
    final var properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    // Read from earliest each time
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    if (token != null) {
      configureSasl(properties, token);
    }
    return new KafkaConsumer<>(properties);
  }

  public AdminClient newAdmin() {
    final var properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    if (token != null) {
      configureSasl(properties, token);
    }
    return KafkaAdminClient.create(properties);
  }

  private static void configureSasl(Properties properties, String token) {
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        String.format(
            """
                              org.apache.kafka.common.security.plain.PlainLoginModule \
                              required username="user" password="token:%s";
                              """,
            token));
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new App()).execute(args));
  }
}
