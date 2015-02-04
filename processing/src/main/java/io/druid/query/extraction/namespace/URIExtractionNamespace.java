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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.net.URI;

/**
 *
 */
@JsonTypeName("uri")
public class URIExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final String namespace;
  @JsonProperty
  private final URI uri;
  @JsonProperty
  private final Boolean isSmile;
  @JsonCreator
  public URIExtractionNamespace(
      @NotNull @JsonProperty(value = "namespace", required = true)
      String namespace,
      @NotNull @JsonProperty(value = "uri", required = true)
      URI uri,
      @Nullable @JsonProperty(value = "isSmile", required = false)
      Boolean isSmile
  ){
    Preconditions.checkNotNull(namespace);
    Preconditions.checkNotNull(uri);
    this.namespace = namespace;
    this.uri = uri;
    this.isSmile = isSmile == null ? false : isSmile;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
  }

  public URI getUri(){
    return uri;
  }

  public Boolean getIsSmile(){
    return isSmile;
  }

  @Override
  public String toString(){
    return String.format("URIExtractionNamespace = { namespace = %s, uri = %s, isSmile = %s }", namespace, uri.toString(), isSmile.toString());
  }
}
