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

package io.druid.server.http;

import com.google.inject.Inject;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditableConfig;
import io.druid.metadata.MetadataRuleManager;
import io.druid.metadata.RulesConfig;
import io.druid.server.coordinator.rules.Rule;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 */
@Path("/druid/coordinator/v1/rules")
public class RulesResource
{
  private final MetadataRuleManager databaseRuleManager;

  @Inject
  public RulesResource(
      MetadataRuleManager databaseRuleManager
  )
  {
    this.databaseRuleManager = databaseRuleManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRules()
  {
    return Response.ok(databaseRuleManager.getAllRules()).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full
  )
  {
    if (full != null) {
      return Response.ok(databaseRuleManager.getRulesWithDefault(dataSourceName))
                     .build();
    }
    return Response.ok(databaseRuleManager.getRules(dataSourceName))
                   .build();
  }

  @Deprecated
  // kept for backward compatibility
  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      final List<Rule> rules
  )
  {
    if (databaseRuleManager.overrideRule(dataSourceName, new RulesConfig(rules, new AuditInfo("NULL","NULL", "NULL")))) {
      return Response.ok().build();
    }
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
  }

  @POST
  @Path("/{dataSourceName}/rulesConfig")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      final RulesConfig rulesConfig
  )
  {
    if (databaseRuleManager.overrideRule(dataSourceName, rulesConfig)) {
      return Response.ok().build();
    }
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
  }

}
