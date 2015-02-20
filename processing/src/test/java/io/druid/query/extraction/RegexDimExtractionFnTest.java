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

import com.google.common.collect.Sets;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class RegexDimExtractionFnTest
{
  private static final String[] paths = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private static final String[] testStrings = {
      "apple",
      "awesome",
      "asylum",
      "business",
      "be",
      "cool"
  };

  @Test
  public void testPathExtraction()
  {
    String regex = "/([^/]+)/";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(dimExtractionFn.getExtractionFunction().apply(path));
    }

    Assert.assertEquals(2, extracted.size());
    Assert.assertTrue(extracted.contains("druid"));
    Assert.assertTrue(extracted.contains("dash"));
  }

  @Test
  public void testDeeperPathExtraction()
  {
    String regex = "^/([^/]+/[^/]+)(/|$)";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(dimExtractionFn.getExtractionFunction().apply(path));
    }

    Assert.assertEquals(4, extracted.size());
    Assert.assertTrue(extracted.contains("druid/prod"));
    Assert.assertTrue(extracted.contains("druid/demo"));
    Assert.assertTrue(extracted.contains("dash/aloe"));
    Assert.assertTrue(extracted.contains("dash/baloo"));
  }

  @Test
  public void testStringExtraction()
  {
    String regex = "(.)";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String testString : testStrings) {
      extracted.add(dimExtractionFn.getExtractionFunction().apply(testString));
    }

    Assert.assertEquals(3, extracted.size());
    Assert.assertTrue(extracted.contains("a"));
    Assert.assertTrue(extracted.contains("b"));
    Assert.assertTrue(extracted.contains("c"));
  }
}
