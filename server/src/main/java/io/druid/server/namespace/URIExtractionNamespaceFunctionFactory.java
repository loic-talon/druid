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

package io.druid.server.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 */
public class URIExtractionNamespaceFunctionFactory implements ExtractionNamespaceFunctionFactory<URIExtractionNamespace>
{
  private static final Logger log = new Logger(URIExtractionNamespaceFunctionFactory.class);
  private final NamespaceExtractionCacheManager extractionCacheManager;
  private final ObjectMapper smileMapper;
  private final ObjectMapper jsomMapper;
  private final Map<String, DataSegmentPuller> pullers;

  @Inject
  public URIExtractionNamespaceFunctionFactory(
      NamespaceExtractionCacheManager extractionCacheManager,
      @Smile ObjectMapper smileMapper,
      @Json ObjectMapper jsonMapper,
      Map<String, DataSegmentPuller> pullers
  )
  {
    this.extractionCacheManager = extractionCacheManager;
    this.smileMapper = smileMapper;
    this.jsomMapper = jsonMapper;
    this.pullers = pullers;
  }

  @Override
  public Function<String, String> build(final URIExtractionNamespace extractionNamespace)
  {
    final ConcurrentMap<String, String> cache = extractionCacheManager.getCacheMap(extractionNamespace.getNamespace());
    return new Function<String, String>()
    {
      @Nullable
      @Override
      public String apply(String input)
      {
        if (input == null) {
          return null;
        }
        return cache.get(input);
      }
    };
  }

  @Override
  public Runnable getCachePopulator(final URIExtractionNamespace extractionNamespace)
  {
    return new Runnable()
    {
      private volatile long lastCached = JodaUtils.MIN_INSTANT;
      @Override
      public void run()
      {
        final URI uri = extractionNamespace.getUri();
        final DataSegmentPuller puller = pullers.get(uri.getScheme());
        final String uriPath = uri.getPath();
        if (puller == null) {
          throw new IAE(
              "Unknown loader type[%s].  Known types are %s",
              uri.getScheme(),
              pullers.keySet()
          );
        }
        // Inspired by OmniSegmentLoader
        try {
          RetryUtils.retry(
              new Callable<Void>()
              {
                @Override
                public Void call() throws Exception
                {
                  Long lastModified = puller.getLastModified(uri);
                  if(null != lastModified){
                    if(lastModified <= lastCached){
                      final DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                      log.info(
                          "URI [%s] for namespace [%s] was las modified [%s] but was last cached [%s]. Skipping ",
                          uri.toString(),
                          extractionNamespace.getNamespace(),
                          fmt.print(lastModified),
                          fmt.print(lastCached)
                      );
                      return null;
                    }
                  }
                  try (InputStream in = puller.getUriInputStream(uri)) {

                    if (uriPath.endsWith(".zip")) {
                      // Open Zip stream and take the first entry
                      log.debug("Loading zip uri [%s]", uri.toString());
                      try (ZipInputStream zipStream = new ZipInputStream(in)) {
                        final ZipEntry entry = zipStream.getNextEntry();
                        if (entry != null) {
                          log.debug("Found zip entry [%s] of size [%d]", entry.getName(), entry.getSize());
                          if (entry.isDirectory()) {
                            throw new IAE(
                                "Found directory [%s] in zip URI [%s] instead of a file",
                                entry.getName(),
                                uri.toString()
                            );
                          }
                          extractionCacheManager.streamFill(
                              zipStream,
                              extractionNamespace.getNamespace(),
                              extractionNamespace.getIsSmile() ? smileMapper : jsomMapper
                          );
                        }
                      }
                    } else if (uriPath.endsWith(".gz")) {
                      // Simple gzip stream
                      log.debug("Loading gz");
                      try (InputStream gzStream = new GZIPInputStream(in)) {
                        extractionCacheManager.streamFill(
                            gzStream,
                            extractionNamespace.getNamespace(),
                            extractionNamespace.getIsSmile() ? smileMapper : jsomMapper
                        );
                      }
                    } else {
                      // Use the uri stream directly
                      extractionCacheManager.streamFill(
                          in,
                          extractionNamespace.getNamespace(),
                          extractionNamespace.getIsSmile() ? smileMapper : jsomMapper
                      );
                    }
                    log.info("Finished loading namespace [%s]", extractionNamespace.getNamespace());
                    if(lastModified != null) {
                      lastCached = lastModified;
                    }
                  }
                  return null;
                }
              },
              puller.buildShouldRetryPredicate(),
              10
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
