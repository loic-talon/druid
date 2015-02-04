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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.namespace.NamespacedExtractionModule;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractionModuleTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();
  private static final ZkPathsConfig zkPathsConfig = new ZkPathsConfig();
  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private Injector injector;
  private NamespaceExtractionCacheManager cacheManager;
  private Lifecycle lifecycle;
  private NamespacedExtractionModule.NamespacedKeeper keeper;
  private ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();
  @Before
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    cacheManager = new OnHeapNamespaceExtractionCacheManager(lifecycle);
    injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(NamespaceExtractionCacheManager.class).toInstance(cacheManager);
            binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(mapper);
            binder.bind(Key.get(ObjectMapper.class, Smile.class)).toInstance(mapper);
            binder
                .bind(ExtractionNamespaceFunctionFactory.class)
                .annotatedWith(Names.named(JDBCExtractionNamespace.class.getCanonicalName()))
                .to(JDBCExtractionNamespaceFunctionFactory.class);
            binder
                .bind(ExtractionNamespaceFunctionFactory.class)
                .annotatedWith(Names.named(URIExtractionNamespace.class.getCanonicalName()))
                .to(URIExtractionNamespaceFunctionFactory.class);
            MapBinder<String, DataSegmentPuller> mapBindings = MapBinder.newMapBinder(binder, String.class, DataSegmentPuller.class);
            mapBindings.addBinding("file").toInstance(new LocalDataSegmentPuller());
          }
        }
    );
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.create().creatingParentsIfNeeded().forPath(zkPathsConfig.getNamespacePath());
    fnCache.clear();
    keeper = new NamespacedExtractionModule.NamespacedKeeper(
        cf,
        zkPathsConfig,
        mapper,
        injector,
        cacheManager,
        fnCache,
        new NamespacedExtractionModule.NamespacedExtractionModuleConfig(true)
    );
    keeper.start();
  }

  @After
  public void tearDown() throws Exception
  {
    keeper.stop();
    lifecycle.stop();
    cf.close();
    testingCluster.stop();
  }
  
  @Test
  public void testNewTask() throws IOException
  {
    final File tmpFile =  Files.createTempFile("druidTest", "renameTmp").toFile();
    tmpFile.deleteOnExit();
    try(OutputStreamWriter out = new FileWriter(tmpFile)){
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespaceFunctionFactory factory = new URIExtractionNamespaceFunctionFactory(
        cacheManager,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        ImmutableMap.<String, DataSegmentPuller>of("file", new LocalDataSegmentPuller())
    );
    final URIExtractionNamespace namespace = new URIExtractionNamespace("ns", tmpFile.toURI(), false);
    final Function<String, String> fn = factory.build(namespace);
    factory.getCachePopulator(namespace).run();
    Assert.assertEquals("bar", fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));
  }
}
