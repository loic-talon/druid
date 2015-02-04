/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.druid.guice.annotations.Json;
import io.druid.jackson.DefaultObjectMapper;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class ExplicitDimRenameFn implements DimExtractionFn
{
  @JsonIgnore
  private final ObjectMapper mapper;

  private final Map<String, String> renames;
  private final Function<String, String> extractionFunction;

  @JsonCreator
  public ExplicitDimRenameFn(
      @JsonProperty("renames") final Map<String, String> renames
  )
  {
    this.mapper = new DefaultObjectMapper();
    this.renames = renames;
    this.extractionFunction = new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable String input)
      {
        final String retval = renames.get(input);
        return Strings.isNullOrEmpty(retval) ? input : retval;
      }
    };
  }

  private static final byte CACHE_TYPE_ID = 0x5;

  @JsonProperty("renames")
  public Map<String, String> getRenames()
  {
    return renames;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] bytes;
    try {
      bytes = mapper.writeValueAsBytes(renames);
    }
    catch (JsonProcessingException e) {
      // Should never happen. If it can't map a Map<String, String> then there's something weird
      throw Throwables.propagate(e);
    }

    return ByteBuffer
        .allocate(bytes.length + 1)
        .put(CACHE_TYPE_ID)
        .put(bytes)
        .array();
  }

  @Override
  public Function<String, String> getExtractionFunction()
  {
    return this.extractionFunction;
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }
}
