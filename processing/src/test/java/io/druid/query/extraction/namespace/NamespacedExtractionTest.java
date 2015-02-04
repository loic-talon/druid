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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import io.druid.query.extraction.NamespacedExtraction;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractionTest
{
  private static final ConcurrentMap<String, Function<String, String>> defaultMap = new ConcurrentHashMap<>();
  private static final Function<String, String> NOOP_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return Strings.isNullOrEmpty(input) ? null : input;
    }
  };
  private static final Function<String, Function<String, String>> defaultFnFinder = new Function<String, Function<String, String>>()
  {
    @Nullable
    @Override
    public Function<String, String> apply(@Nullable String input)
    {
      Function<String, String> fn = defaultMap.get(input);
      return fn == null ? NOOP_FN : fn;
    }
  };
  @BeforeClass
  public static void setupStatic()
  {
    defaultMap.put(
        "noop", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(String input)
          {
            return input;
          }
        }
    );
    defaultMap.put(
        "null", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return null;
          }
        }
    );
    defaultMap.put(
        "turtles", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return "turtle";
          }
        }
    );
    defaultMap.put(
        "empty", new Function<String, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable String input)
          {
            return "";
          }
        }
    );
  }

  @Test
  public void testSimpleNamespace()
  {
    NamespacedExtraction namespacedExtraction = new NamespacedExtraction(defaultFnFinder, "noop");
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals(val, namespacedExtraction.getExtractionFunction().apply(val));
    }
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(""));
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(null));
  }

  @Test
  public void testUnknownNamespace()
  {
    NamespacedExtraction namespacedExtraction = new NamespacedExtraction(defaultFnFinder, "HFJDKSHFUINEWUINIUENFIUENFUNEWI");
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals(val, namespacedExtraction.getExtractionFunction().apply(val));
    }
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(""));
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(null));
  }

  @Test
  public void testTurtles()
  {
    NamespacedExtraction namespacedExtraction = new NamespacedExtraction(defaultFnFinder, "turtles");
    for (int i = 0; i < 10; ++i) {
      final String val = UUID.randomUUID().toString();
      Assert.assertEquals("turtle", namespacedExtraction.getExtractionFunction().apply(val));
    }
    Assert.assertEquals("turtle", namespacedExtraction.getExtractionFunction().apply(""));
    Assert.assertEquals("turtle", namespacedExtraction.getExtractionFunction().apply(null));
  }

  @Test
  public void testEmpty()
  {
    NamespacedExtraction namespacedExtraction = new NamespacedExtraction(defaultFnFinder, "empty");
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(""));
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(null));
  }

  @Test
  public void testNull()
  {
    NamespacedExtraction namespacedExtraction = new NamespacedExtraction(defaultFnFinder, "null");
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(""));
    Assert.assertNull(namespacedExtraction.getExtractionFunction().apply(null));
  }
}