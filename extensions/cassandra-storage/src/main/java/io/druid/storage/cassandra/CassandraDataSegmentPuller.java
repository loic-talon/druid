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

package io.druid.storage.cassandra;

import com.google.common.base.Predicate;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.UOE;
import com.metamx.common.logger.Logger;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Cassandra Segment Puller
 *
 * @author boneill42
 */
public class CassandraDataSegmentPuller extends CassandraStorage implements DataSegmentPuller
{
  public static final String scheme = "cassandra";
  private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
  private static final int CONCURRENCY = 10;
  private static final int BATCH_SIZE = 10;

  @Inject
  public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
  {
    super(config);
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {
    String key = (String) segment.getLoadSpec().get("key");
    log.info("Pulling index from C* at path[%s] to outDir[%s]", key, outDir);

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }

    long startTime = System.currentTimeMillis();
    ObjectMetadata meta = null;
    final File outFile = new File(outDir, "index.zip");
    try {
      try {
        log.info("Writing to [%s]", outFile.getAbsolutePath());
        OutputStream os = Files.newOutputStreamSupplier(outFile).getOutput();
        meta = ChunkedStorage
            .newReader(indexStorage, key, os)
            .withBatchSize(BATCH_SIZE)
            .withConcurrencyLevel(CONCURRENCY)
            .call();
        os.close();
        CompressionUtils.unzip(outFile, outDir);
      }
      catch (Exception e) {
        FileUtils.deleteDirectory(outDir);
      }
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
    log.info(
        "Pull of file[%s] completed in %,d millis (%s bytes)", key, System.currentTimeMillis() - startTime,
        meta.getObjectSize()
    );
  }

  @Override
  public void getSegmentFiles(URI uri, File file) throws SegmentLoadingException
  {
    throw new SegmentLoadingException(
        new UOE("Cassandra generic URI loading not supported"),
        "Cannot load uri [%s]",
        uri.toString()
    );
  }

  @Override
  public InputStream getUriInputStream(final URI uri) throws SegmentLoadingException
  {
    if (!uri.getScheme().equalsIgnoreCase(scheme)) {
      throw new SegmentLoadingException("Cannot load segment scheme [%s]", uri.getScheme());
    }
    final File tmpFile;
    try {
      tmpFile = File.createTempFile("cassandra", ".cache");
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to create cache file");
    }
    tmpFile.deleteOnExit();

    try (final OutputStream os = new FileOutputStream(tmpFile)) {
      ObjectMetadata meta = ChunkedStorage
          .newReader(indexStorage, uri.getPath(), os)
          .withBatchSize(BATCH_SIZE)
          .withConcurrencyLevel(CONCURRENCY)
          .call();
      return new FileInputStream(tmpFile)
      {
        @Override
        public void close() throws IOException
        {
          IOException ex = null;
          try {
            super.close();
          }
          catch (IOException e) {
            ex = e;
          }
          if (tmpFile.exists() && !tmpFile.delete()) {
            final IOException subEx = new IOException("Could not delete file " + tmpFile.getAbsolutePath());
            if (ex == null) {
              ex = subEx;
            } else {
              ex.addSuppressed(subEx);
            }
          }
          if (ex != null) {
            log.error(ex, "Error closing cache [%s] for URI [%s]", tmpFile.getAbsolutePath(), uri.toString());
            throw ex;
          }
        }
      };
    }
    catch (Exception e) {
      throw new SegmentLoadingException(
          e,
          "Could not load segment from URI [%s] using [%s] as a temporary cache",
          uri.toString(),
          tmpFile.getAbsolutePath()
      );
    }
  }

  @Override
  public Predicate<Throwable> buildShouldRetryPredicate()
  {
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable input)
      {
        return false;
      }
    };
  }

  @Override
  public Long getLastModified(URI uri) throws SegmentLoadingException
  {
    return null;
  }
}
