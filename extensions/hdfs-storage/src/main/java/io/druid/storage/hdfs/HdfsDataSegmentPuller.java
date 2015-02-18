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

package io.druid.storage.hdfs;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import com.metamx.common.UOE;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.tools.FileObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;

/**
 */
public class HdfsDataSegmentPuller implements DataSegmentPuller
{
  public static class HdfsDataSegmentPullerException extends RuntimeException
  {
    public HdfsDataSegmentPullerException(Throwable ex)
    {
      super(ex);
    }
  }


  public static FileObject buildFileObject(final URI uri, final Configuration config)
  {
    return buildFileObject(uri, config, false);
  }

  public static FileObject buildFileObject(final URI uri, final Configuration config, final Boolean overwrite)
  {
    return new FileObject()
    {
      final Path path = new Path(uri);

      @Override
      public URI toUri()
      {
        return uri;
      }

      @Override
      public String getName()
      {
        return path.getName();
      }

      @Override
      public InputStream openInputStream() throws IOException
      {
        final FileSystem fs = path.getFileSystem(config);
        return fs.open(path);
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        final FileSystem fs = path.getFileSystem(config);
        return fs.create(path, overwrite);
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("HDFS Reader not supported");
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("HDFS CharSequence not supported");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        throw new UOE("HDFS Writer not supported");
      }

      @Override
      public long getLastModified()
      {
        try {
          final FileSystem fs = path.getFileSystem(config);
          return fs.getFileStatus(path).getModificationTime();
        }
        catch (IOException ex) {
          throw new HdfsDataSegmentPullerException(ex);
        }
      }

      @Override
      public boolean delete()
      {
        try {
          final FileSystem fs = path.getFileSystem(config);
          return fs.delete(path, false);
        }
        catch (IOException ex) {
          throw new HdfsDataSegmentPullerException(ex);
        }
      }
    };
  }

  public static final String scheme = HdfsStorageDruidModule.scheme;
  private static final Logger log = new Logger(HdfsDataSegmentPuller.class);
  private final Configuration config;

  @Inject
  public HdfsDataSegmentPuller(final Configuration config)
  {
    this.config = config;
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    final Path path = getPath(segment);

    final FileSystem fs = checkPathAndGetFilesystem(path);

    if (path.getName().endsWith(".zip")) {
      try {
        try (FSDataInputStream in = fs.open(path)) {
          CompressionUtils.unzip(in, dir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Some IOException");
      }
    } else {
      throw new SegmentLoadingException("Unknown file type[%s]", path);
    }
  }

  @Override
  public void getSegmentFiles(URI uri, File outDir) throws SegmentLoadingException
  {
    if (!uri.getScheme().equalsIgnoreCase(scheme)) {
      throw new SegmentLoadingException("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    final Path path = new Path(uri);
    try {
      final FileSystem fs = path.getFileSystem(config);

      if (!fs.isDirectory(path)) {
        throw new SegmentLoadingException("URI [%s] is not a directory", uri.toString());
      }

      final RemoteIterator<LocatedFileStatus> children = fs.listFiles(path, false);

      while (children.hasNext()) {
        final LocatedFileStatus child = children.next();
        final Path childPath = child.getPath();
        final String fname = childPath.getName();

        if (fs.isDirectory(childPath)) {
          log.warn("[%s] is a directory, skipping", childPath.toString());
        } else {
          fs.copyToLocalFile(childPath, new Path(new File(outDir, fname).toURI()));
        }
      }
    }
    catch (FileNotFoundException e) {
      throw new SegmentLoadingException(e, "Could not find file or child at uri [%s].", uri.toString());
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Error in copying uri [%s] to [%s]", uri.toString(), outDir.toString());
    }
  }

  @Override
  public InputStream getUriInputStream(URI uri) throws SegmentLoadingException
  {
    if (!uri.getScheme().equalsIgnoreCase(scheme)) {
      throw new SegmentLoadingException("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    Path path = new Path(uri);
    try {
      return buildFileObject(uri, config).openInputStream();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Failed to open HDFS stream at uri [%s]", uri.toString());
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
        // TODO: smarter retries
        return false;
      }
    };
  }

  @Override
  public Long getLastModified(URI uri) throws SegmentLoadingException
  {
    try {
      return buildFileObject(uri, config).getLastModified();
    }
    catch (HdfsDataSegmentPullerException ex) {
      throw new SegmentLoadingException(ex.getCause(), "Error getting modified date for HDFS URI [%s]", uri.toString());
    }
  }

  private Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get("path")));
  }

  private FileSystem checkPathAndGetFilesystem(Path path) throws SegmentLoadingException
  {
    FileSystem fs;
    try {
      fs = path.getFileSystem(config);

      if (!fs.exists(path)) {
        throw new SegmentLoadingException("Path[%s] doesn't exist.", path);
      }

      return fs;
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problems interacting with filesystem[%s].", path);
    }
  }
}
