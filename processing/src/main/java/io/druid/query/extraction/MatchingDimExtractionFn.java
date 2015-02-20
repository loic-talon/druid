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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.metamx.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class MatchingDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String expr;
  private final Pattern pattern;
  private final Function<String, String> extractionFunction;

  @JsonCreator
  public MatchingDimExtractionFn(
      @JsonProperty("expr") String expr
  )
  {
    this.expr = expr;
    this.pattern = Pattern.compile(expr);
    this.extractionFunction = new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String dimValue)
      {
        dimValue = (dimValue == null) ? "" : dimValue;
        Matcher matcher = pattern.matcher(dimValue);
        return matcher.find() ? dimValue : null;
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = StringUtils.toUtf8(expr);
    return ByteBuffer.allocate(1 + exprBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(exprBytes)
                     .array();
  }

  @Override
  public Function<String, String> getExtractionFunction(){
    return this.extractionFunction;
  }

  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return String.format("regex_matches(%s)", expr);
  }
}
