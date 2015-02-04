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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.IAE;
import com.metamx.common.concurrent.ExecutorServices;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = OnHeapNamespaceExtractionCacheManager.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "offHeap", value = OffHeapNamespaceExtractionCacheManager.class),
    @JsonSubTypes.Type(name = "onHeap", value = OnHeapNamespaceExtractionCacheManager.class),
    @JsonSubTypes.Type(name = "cache", value = ClientCacheExtractionCacheManager.class)
})
public abstract class NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(NamespaceExtractionCacheManager.class);
  private final ListeningScheduledExecutorService listeningScheduledExecutorService;
  private final ConcurrentMap<String, Collection<ListenableScheduledFuture<?>>> repeatFutures = new ConcurrentHashMap<>();

  public NamespaceExtractionCacheManager(
      Lifecycle lifecycle
  )
  {
    this.listeningScheduledExecutorService = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("NamespaceExtractionCacheManager-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    ExecutorServices.manageLifecycle(lifecycle, listeningScheduledExecutorService);
  }

  public ListenableFuture<?> scheduleOnce(final String ns, final Runnable runnable)
  {
    final Collection<ListenableScheduledFuture<?>> futures = repeatFutures.get(ns);
    if (futures != null) {
      log.warn("Namespace [%s] has repeated updates but was requested to schedule a one-off", ns);
    }
    return scheduleOnce(runnable);
  }

  public ListenableScheduledFuture<?> scheduleRepeat(
      final String ns,
      final Runnable command,
      final long delay,
      final TimeUnit timeUnit
  )
  {
    Collection<ListenableScheduledFuture<?>> futures = repeatFutures.get(ns);
    if (futures == null) {
      repeatFutures.putIfAbsent(ns, new ConcurrentLinkedQueue<ListenableScheduledFuture<?>>());
      futures = repeatFutures.get(ns);
    } else {
      log.warn("Namespace [%s] has repeated updates but was requested to schedule a repeated update", ns);
    }
    ListenableScheduledFuture<?> future = listeningScheduledExecutorService.scheduleAtFixedRate(
        command,
        0,
        delay,
        timeUnit
    );
    futures.add(future);
    return future;
  }

  protected <T> ListenableFuture<T> scheduleOnce(
      final Callable<T> command
  )
  {
    return listeningScheduledExecutorService.submit(command);
  }

  protected ListenableFuture<?> scheduleOnce(
      Runnable runnable
  )
  {
    return listeningScheduledExecutorService.submit(runnable);
  }

  protected ListenableScheduledFuture<?> scheduleRepeat(
      final Runnable command, final long delay, final TimeUnit timeUnit
  )
  {
    return listeningScheduledExecutorService.scheduleAtFixedRate(command, 0, delay, timeUnit);
  }

  public abstract ConcurrentMap<String, String> getCacheMap(String namespace);

  public void delete(String ns)
  {
    Collection<ListenableScheduledFuture<?>> futures = repeatFutures.get(ns);
    if (futures == null) {
      repeatFutures.putIfAbsent(ns, new ConcurrentLinkedQueue<ListenableScheduledFuture<?>>());
      futures = repeatFutures.get(ns);
    }
    Futures.allAsList(futures).cancel(true);
    repeatFutures.remove(ns);
  }

  public abstract Collection<String> getKnownNamespaces();

  public void streamFill(final InputStream stream, final String ns, final ObjectMapper objectMapper) throws IOException
  {
    final Map<String, String> map = getCacheMap(ns);
    try (JsonParser jp = objectMapper.getFactory().createParser(stream)) {
      JsonToken token = jp.nextToken();
      if (token != JsonToken.START_OBJECT) {
        throw new IAE("Could not find start of object");
      }
      String key = null;
      String value = null;
      while (null != token) {
        switch (token) {
          case FIELD_NAME:
            key = jp.getText();
            break;
          case VALUE_STRING:
            value = jp.getText();
            break;
          case START_OBJECT:
            if (key != null || value != null) {
              throw new IAE("Reached start of object too late");
            }
            break;
          default:
            throw new IAE("Failed to parse objects. Found an unexpected [%s]", token);
        }
        if (key != null && value != null) {
          map.put(key, value);
          key = null;
          value = null;
        }
        token = jp.nextToken();
        if (token == JsonToken.END_OBJECT) {
          if (key != null || value != null) {
            throw new IAE("Reached end of object too soon");
          }
          break;
        }
      }
    }
  }
}