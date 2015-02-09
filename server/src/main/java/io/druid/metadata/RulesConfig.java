/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditableConfig;
import io.druid.server.coordinator.rules.Rule;

import java.util.List;

public class RulesConfig implements AuditableConfig
{
  private final List<Rule> rules;

  private final AuditInfo auditInfo;

  @JsonCreator
  public RulesConfig(
      @JsonProperty("rules") List<Rule> rules,
      @JsonProperty("auditInfo") AuditInfo auditInfo
  ){
    Preconditions.checkNotNull(rules);
    Preconditions.checkNotNull(auditInfo);

    this.rules = rules;
    this.auditInfo = auditInfo;
  }

  @JsonProperty
  public AuditInfo getAuditInfo()
  {
    return auditInfo;
  }

  @JsonProperty
  public List<Rule> getRules(){
    return rules;
  }
}
