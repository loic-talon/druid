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

package io.druid.query.extraction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class ExplicitRenameFnTest
{
  private String serialized = "";
  private ObjectMapper mapper;
  private static final Map<String, String> renames = ImmutableMap.of(
      "foo", "bar",
      "bar", "baz"
  );

  @Before
  public void setup() throws JsonProcessingException
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
    serialized = String.format(
        "{\"type\":\"explicitRename\",\"renames\":%s}",
        mapper.writeValueAsString(renames)
    );
  }

  @Test
  public void testDeserialization() throws IOException
  {
    final DimExtractionFn fn = mapper.reader(DimExtractionFn.class).readValue(serialized);
    for (String key : renames.keySet()) {
      Assert.assertEquals(renames.get(key), fn.getExtractionFunction().apply(key));
    }
    final String crazyString = UUID.randomUUID().toString();
    Assert.assertEquals(crazyString, fn.getExtractionFunction().apply(crazyString));
  }
}
