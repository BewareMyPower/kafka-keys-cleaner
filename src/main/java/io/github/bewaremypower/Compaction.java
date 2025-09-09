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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.Cleanup;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

@Command(name = "compact", description = "Produces messages.")
public class Compaction implements Callable<Integer> {

  private static final String CLEANUP_POLICY_KEY = "cleanup.policy";
  private static final String CLEANUP_POLICY_VALUE = "compact";

  @ParentCommand App app;

  @Option(
      names = {"--apply"},
      description = "Enable compaction for the topic")
  boolean enableCompaction;

  @Override
  public Integer call() throws Exception {
    @Cleanup final var admin = app.newAdmin();
    if (enableCompaction) {
      final var resource = new ConfigResource(ConfigResource.Type.TOPIC, app.getTopic());
      final var configs = admin.describeConfigs(List.of(resource)).all().get();
      final var describedConfigs = configs.get(resource);
      final var optConfigEntry =
          describedConfigs.entries().stream()
              .filter(__ -> __.name().equals(CLEANUP_POLICY_KEY))
              .findAny();
      optConfigEntry.ifPresent(
          entry -> System.out.println(CLEANUP_POLICY_KEY + " is " + entry.value()));

      if (!optConfigEntry.map(ConfigEntry::value).orElse("").equals(CLEANUP_POLICY_VALUE)) {
        System.out.println("Updating " + CLEANUP_POLICY_KEY + " with " + CLEANUP_POLICY_VALUE);
        admin
            .incrementalAlterConfigs(
                Map.of(
                    resource,
                    List.of(
                        new AlterConfigOp(
                            new ConfigEntry(CLEANUP_POLICY_KEY, CLEANUP_POLICY_VALUE),
                            AlterConfigOp.OpType.SET))))
            .all()
            .get();
      }
    }
    return 0;
  }
}
