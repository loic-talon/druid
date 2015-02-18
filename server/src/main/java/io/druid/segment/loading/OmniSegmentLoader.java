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

package io.druid.segment.loading;

import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.RetryUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.SegmentMissingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

/**
 */
public class OmniSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(OmniSegmentLoader.class);

  private final Map<String, DataSegmentPuller> pullers;
  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;

  private final List<StorageLocation> locations;

  private final Object lock = new Object();

  @Inject
  public OmniSegmentLoader(
      Map<String, DataSegmentPuller> pullers,
      QueryableIndexFactory factory,
      SegmentLoaderConfig config
  )
  {
    this.pullers = pullers;
    this.factory = factory;
    this.config = config;

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
  }

  public OmniSegmentLoader withConfig(SegmentLoaderConfig config)
  {
    return new OmniSegmentLoader(pullers, factory, config);
  }

  @Override
  public boolean isSegmentLoaded(final DataSegment segment)
  {
    return findStorageLocationIfLoaded(segment) != null;
  }

  public StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : locations) {
      File localStorageDir = new File(location.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
      if (localStorageDir.exists()) {
        return location;
      }
    }
    return null;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    File segmentFiles = getSegmentFiles(segment);
    final QueryableIndex index = factory.factorize(segmentFiles);

    return new QueryableIndexSegment(segment.getIdentifier(), index);
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    StorageLocation loc = findStorageLocationIfLoaded(segment);

    final File retVal;

    if (loc == null) {
      Iterator<StorageLocation> locIter = locations.iterator();
      loc = locIter.next();
      while (locIter.hasNext()) {
        loc = loc.mostEmpty(locIter.next());
      }

      if (!loc.canHandle(segment.getSize())) {
        throw new ISE(
            "Segment[%s:%,d] too large for storage[%s:%,d].",
            segment.getIdentifier(), segment.getSize(), loc.getPath(), loc.available()
        );
      }

      File storageDir = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));

      // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
      // the parent directories of the segment are removed
      final File downloadStartMarker = new File(storageDir, "downloadStartMarker");
      synchronized (lock) {
        if (!storageDir.mkdirs()) {
          log.debug("Unable to make parent file[%s]", storageDir);
        }
        try {
          if(!downloadStartMarker.createNewFile()){
            // TODO: should this fail?
            log.warn("Was not able to create new download marker for [%s]", storageDir);
          }
        }
        catch (IOException e) {
          throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
        }
      }

      if(segment.getLoadSpec().containsKey("uri")){
        loadURI(segment.getLoadSpec(), storageDir);
      } else {
        // legacy
        getPuller(segment.getLoadSpec()).getSegmentFiles(segment, storageDir);
      }

      if (!downloadStartMarker.delete()) {
        throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
      }

      retVal = storageDir;
    } else {
      retVal = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
    }

    loc.addSegment(segment);

    return retVal;
  }

  protected void loadURI(Map<String, Object> loadSpec, final File outDir) throws SegmentLoadingException
  {
    final URI uri = MapUtils.getURI(loadSpec, "uri");
    log.debug("Loading URI [%s]", uri.toString());
    final DataSegmentPuller puller = pullers.get(uri.getScheme());
    if (puller == null) {
      throw new SegmentLoadingException(
          "Unknown loader type[%s].  Known types are %s",
          uri.getScheme(),
          pullers.keySet()
      );
    }
    // We don't actually want to use this as a local file, but we want to be able to parse the path
    final File file = new File(uri.getPath());
    final String fileName = file.getName();
    final long startTime = System.currentTimeMillis();
    try {
      RetryUtils.retry(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {

              if (fileName.endsWith(".zip")) {
                log.debug("Loading zip uri [%s]", uri.toString());
                try (InputStream in = puller.getUriInputStream(uri)) {
                  CompressionUtils.unzip(in, outDir);
                }
                catch (IOException e) {
                  throw new SegmentLoadingException(e, "Error pulling zip uri [%s]", uri.toString());
                }
              } else if (fileName.endsWith(".gz")) {
                log.debug("Loading gz");
                final File outFile = new File(outDir, fileName.substring(0, fileName.length() - ".gz".length()));
                final ByteSink byteSink = Files.asByteSink(outFile);
                try (InputStream in = puller.getUriInputStream(uri)) {
                  byteSink.writeFrom(new GZIPInputStream(in));
                }
                catch (IOException e) {
                  throw new SegmentLoadingException(e, "Error pulling gz uri [%s]", uri.toString());
                }
              } else {
                // Try and let the puller resolve it (example: is a directory)
                puller.getSegmentFiles(uri, outDir);
              }
              return null;
            }
          },
          puller.buildShouldRetryPredicate(),
          10
      );
    }
    catch (Exception e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] after exception",
            outDir
        );
        e.addSuppressed(ioe);
      }
      throw new SegmentLoadingException(e, "Could not load segment at uri [%s]", uri.toString());
    }
    log.info("Pulled data from [%s] in %d ms", uri.toString(), System.currentTimeMillis() - startTime);
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    StorageLocation loc = findStorageLocationIfLoaded(segment);

    if (loc == null) {
      log.info("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment);
      return;
    }

    try {
      // Druid creates folders of the form dataSource/interval/version/partitionNum.
      // We need to clean up all these directories if they are all empty.
      File cacheFile = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
      cleanupCacheFiles(loc.getPath(), cacheFile);
      loc.removeSegment(segment);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  @Deprecated
  private DataSegmentPuller getPuller(Map<String, Object> loadSpec) throws SegmentLoadingException
  {
    String type = MapUtils.getString(loadSpec, "type");
    DataSegmentPuller loader = pullers.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, pullers.keySet());
    }

    return loader;
  }

  public void cleanupCacheFiles(File baseFile, File cacheFile) throws IOException
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (lock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error("Unable to remove file[%s]", cacheFile);
      }
    }

    if (cacheFile.getParentFile() != null && cacheFile.getParentFile().listFiles().length == 0) {
      cleanupCacheFiles(baseFile, cacheFile.getParentFile());
    }
  }
}
