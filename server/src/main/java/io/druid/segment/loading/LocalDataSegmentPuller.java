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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.metamx.common.MapUtils;
import com.metamx.common.UOE;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

import javax.tools.FileObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Map;

/**
 */
public class LocalDataSegmentPuller implements DataSegmentPuller
{
  public static FileObject buildFileObject(final URI uri)
  {
    final File file = new File(uri);
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
        return new FileInputStream(file);
      }

      @Override
      public OutputStream openOutputStream() throws IOException
      {
        return new FileOutputStream(file);
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException
      {
        return new FileReader(file);
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException
      {
        throw new UOE("CharSequence not supported");
      }

      @Override
      public Writer openWriter() throws IOException
      {
        return new FileWriter(file);
      }

      @Override
      public long getLastModified()
      {
        return file.lastModified();
      }

      @Override
      public boolean delete()
      {
        return file.delete();
      }
    };
  }

  public static final String scheme = "file";
  private static final Logger log = new Logger(LocalDataSegmentPuller.class);

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    final File path = getFile(segment);

    if (path.isDirectory()) {
      if (path.equals(dir)) {
        log.info("Asked to load [%s] into itself, done!", dir);
        return;
      }

      log.info("Copying files from [%s] to [%s]", path, dir);
      File file = null;
      try {
        final File[] files = path.listFiles();
        for (int i = 0; i < files.length; ++i) {
          file = files[i];
          Files.copy(file, new File(dir, file.getName()));
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to copy file[%s].", file);
      }
    } else {
      if (!path.getName().endsWith(".zip")) {
        throw new SegmentLoadingException("File is not a zip file[%s]", path);
      }

      log.info("Unzipping local file[%s] to [%s]", path, dir);
      try {
        CompressionUtils.unzip(path, dir);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to unzip file[%s]", path);
      }
    }
  }

  @Override
  public void getSegmentFiles(final URI uri, final File dir) throws SegmentLoadingException
  {
    if (!uri.getScheme().equalsIgnoreCase(scheme)) {
      throw new SegmentLoadingException("Don't know how to load scheme for URI [%s]", uri.toString());
    }
    if (!(scheme.equals(uri.getScheme()))) {
      throw new SegmentLoadingException("Cannot load a non file uri: [%s]", uri.toString());
    }
    final File inDir = new File(uri);
    if (!inDir.isDirectory()) {
      throw new SegmentLoadingException("URI [%s] is not a directory", uri.toString());
    }
    try {
      File[] files = inDir.listFiles();
      if (files == null) {
        throw new SegmentLoadingException("Could not find any files in [%s]", uri.toString());
      }
      for (File file : files) {
        Files.copy(file, new File(dir, file.getName()));
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Could not copy file [%s]", uri.toString());
    }
  }

  @Override
  public InputStream getUriInputStream(URI uri) throws SegmentLoadingException
  {
    try {
      return buildFileObject(uri).openInputStream();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Could not open file at [%s]", uri.toString());
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
        // Local failures should probably just hard-fail
        return false;
      }
    };
  }

  @Override
  public Long getLastModified(URI uri)
  {
    return buildFileObject(uri).getLastModified();
  }

  private File getFile(DataSegment segment) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final File path = new File(MapUtils.getString(loadSpec, "path"));

    if (!path.exists()) {
      throw new SegmentLoadingException("Asked to load path[%s], but it doesn't exist.", path);
    }

    return path;
  }
}
