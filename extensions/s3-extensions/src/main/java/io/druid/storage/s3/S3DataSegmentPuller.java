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

package io.druid.storage.s3;

import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.UOE;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import javax.tools.FileObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

/**
 */
public class S3DataSegmentPuller implements DataSegmentPuller
{
  public static FileObject buildFileObject(final URI uri, final RestS3Service s3Client) throws S3ServiceException
  {
    final URI checkedUri = checkURI(uri);
    final AmazonS3URI s3URI = new AmazonS3URI(checkedUri);
    final String key = s3URI.getKey();
    final String bucket = s3URI.getBucket();
    final S3Object s3Obj = s3Client.getObject(bucket, key);
    final String path = uri.getPath();

    return new FileObject()
    {
      @Override
      public URI toUri()
      {
        return uri;
      }

      @Override
      public String getName()
      {
        final String ext = Files.getFileExtension(path);
        return Files.getNameWithoutExtension(path) + (Strings.isNullOrEmpty(ext) ? "" : ("." + ext));
      }

      @Override
      public InputStream openInputStream() throws IOException
      {
        try {
          return s3Obj.getDataInputStream();
        }
        catch (ServiceException e) {
          throw new IOException(String.format("Could not load S3 URI [%s]", checkedUri.toString()), e);
        }
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        throw new UOE("Cannot stream S3 output");
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("Cannot open reader");
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("Cannot open character sequence");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        throw new UOE("Cannot open writer");
      }

      @Override
      public long getLastModified()
      {
        return s3Obj.getLastModifiedDate().getTime();
      }

      @Override
      public boolean delete()
      {
        throw new UOE("Cannot delete S3 items anonymously. jetS3t doesn't support authenticated deletes easily.");
      }
    };
  }

  public static final String scheme = S3StorageDruidModule.segmentFileScheme;

  private static final Logger log = new Logger(S3DataSegmentPuller.class);

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;

  @Inject
  public S3DataSegmentPuller(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException
  {
    final S3Coords s3Coords = new S3Coords(segment);

    log.info("Pulling index at path[%s] to outDir[%s]", s3Coords, outDir);

    if (!isObjectInBucket(s3Coords)) {
      throw new SegmentLoadingException("IndexFile[%s] does not exist.", s3Coords);
    }

    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }

    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              long startTime = System.currentTimeMillis();
              S3Object s3Obj = null;

              try {
                s3Obj = s3Client.getObject(s3Coords.bucket, s3Coords.path);

                try (InputStream in = s3Obj.getDataInputStream()) {
                  final String key = s3Obj.getKey();
                  if (key.endsWith(".zip")) {
                    CompressionUtils.unzip(in, outDir);
                  } else if (key.endsWith(".gz")) {
                    final File outFile = new File(outDir, toFilename(key, ".gz"));
                    ByteStreams.copy(new GZIPInputStream(in), Files.newOutputStreamSupplier(outFile));
                  } else {
                    ByteStreams.copy(in, Files.newOutputStreamSupplier(new File(outDir, toFilename(key, ""))));
                  }
                  log.info(
                      "Pull of file[%s/%s] completed in %,d millis",
                      s3Obj.getBucketName(),
                      s3Obj.getKey(),
                      System.currentTimeMillis() - startTime
                  );
                  return null;
                }
                catch (IOException e) {
                  throw new IOException(String.format("Problem decompressing object[%s]", s3Obj), e);
                }
              }
              finally {
                S3Utils.closeStreamsQuietly(s3Obj);
              }
            }
          }
      );
    }
    catch (Exception e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory for segment[%s] after exception: %s",
            segment.getIdentifier(),
            outDir
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  public static URI checkURI(URI uri)
  {
    if (uri.getScheme().equalsIgnoreCase(scheme)) {
      uri = URI.create("s3" + uri.toString().substring(scheme.length()));
    } else if (!uri.getScheme().equalsIgnoreCase("s3")) {
      throw new IAE("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    return uri;
  }

  @Override
  public void getSegmentFiles(URI uri, final File outDir) throws SegmentLoadingException
  {
    uri = checkURI(uri);
    final AmazonS3URI s3URI = new AmazonS3URI(uri);
    try {
      for (S3Object s3Object : s3Client.listObjects(s3URI.getBucket(), s3URI.getKey(), null)) {
        final File inFile = new File(s3Object.getKey());
        final String fname = inFile.getName();
        final File outFile = new File(outDir, fname);
        final ByteSink byteSink = Files.asByteSink(outFile);
        try (InputStream in = s3Object.getDataInputStream()) {
          byteSink.writeFrom(in);
        }
        catch (ServiceException | IOException e) {
          throw new SegmentLoadingException(e, "Error loading segment at uri [%s]", uri.toString());
        }
      }
    }
    catch (S3ServiceException e) {
      throw new SegmentLoadingException(e, "Error loading segment at uri [%s]", uri.toString());
    }

  }

  @Override
  public InputStream getUriInputStream(URI uri) throws SegmentLoadingException
  {
    try {
      return buildFileObject(uri, s3Client).openInputStream();
    }
    catch (ServiceException | IOException e) {
      throw new SegmentLoadingException(e, "Could not load URI [%s]", uri.toString());
    }
  }

  @Override
  public Predicate<Throwable> buildShouldRetryPredicate()
  {
    return new Predicate<Throwable>()
    {

      @Override
      public boolean apply(Throwable e)
      {
        if (e instanceof IOException) {
          return true;
        } else if (e instanceof ServiceException) {
          final boolean isIOException = e.getCause() instanceof IOException;
          final boolean isTimeout = "RequestTimeout".equals(((ServiceException) e).getErrorCode());
          return isIOException || isTimeout;
        } else {
          return false;
        }
      }
    };
  }

  @Override
  public Long getLastModified(URI uri) throws SegmentLoadingException
  {
    try {
      return buildFileObject(uri, s3Client).getLastModified();
    }
    catch (S3ServiceException e) {
      throw new SegmentLoadingException(e, "Could not fetch last modified timestamp from URI [%s]", uri.toString());
    }
  }

  private String toFilename(String key, final String suffix)
  {
    String filename = key.substring(key.lastIndexOf("/") + 1); // characters after last '/'
    filename = filename.substring(0, filename.length() - suffix.length()); // remove the suffix from the end
    return filename;
  }

  private boolean isObjectInBucket(final S3Coords coords) throws SegmentLoadingException
  {
    try {
      return S3Utils.retryS3Operation(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              return S3Utils.isObjectInBucket(s3Client, coords.bucket, coords.path);
            }
          }
      );
    }
    catch (S3ServiceException | IOException e) {
      throw new SegmentLoadingException(e, "S3 fail! Key[%s]", coords);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static class S3Coords
  {
    String bucket;
    String path;

    public S3Coords(DataSegment segment)
    {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      bucket = MapUtils.getString(loadSpec, BUCKET);
      path = MapUtils.getString(loadSpec, KEY);
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
    }

    public String toString()
    {
      return String.format("s3://%s/%s", bucket, path);
    }
  }
}
