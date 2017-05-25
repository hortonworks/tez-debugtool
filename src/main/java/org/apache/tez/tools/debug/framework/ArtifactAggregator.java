package org.apache.tez.tools.debug.framework;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class ArtifactAggregator implements AutoCloseable {
  private final Configuration conf;
  private final ExecutorService service;
  private final Params params;
  private final CloseableHttpClient httpClient;
  private final Injector injector;
  private final FileSystem zipfs;
  private final List<ArtifactSource> pendingSources;
  private final Map<Artifact, ArtifactSource> artifactSource;

  public ArtifactAggregator(Configuration conf_, ExecutorService service, Params params_,
      String zipFilePath, List<ArtifactSourceType> sourceTypes) throws IOException {
    this.conf = conf_;
    this.service = service;
    this.params = params_;
    this.httpClient = HttpClients.createDefault();
    this.injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(HttpClient.class).toInstance(httpClient);
        binder.bind(Configuration.class).toInstance(conf);
        binder.bind(Params.class).toInstance(params);
      }
    });
    this.zipfs = FileSystems.newFileSystem(URI.create("jar:file:" + zipFilePath),
        ImmutableMap.of("create", "true", "encoding", "UTF-8"));
    this.pendingSources = new ArrayList<>(sourceTypes.size());
    for (ArtifactSourceType sourceType : sourceTypes) {
      pendingSources.add(sourceType.getSource(injector));
    }
    this.artifactSource = new HashMap<>();
  }

  public void aggregate() {
    final Map<String, Exception> errors = new HashMap<>();
    while (!pendingSources.isEmpty()) {
      List<Artifact> artifacts = collectDownloadableArtifacts();
      if (artifacts.isEmpty() && !pendingSources.isEmpty()) {
        // Artifacts is empty, but some sources are pending.
        // Can be because dagId was given, queryId could not be found, etc ...
        break;
      }
      List<Future<?>> futures = new ArrayList<>(artifacts.size());
      for (final Artifact artifact : artifacts) {
        final Path path = zipfs.getPath(artifact.getName());
          futures.add(service.submit(new Runnable() {
            public void run() {
              try {
                artifact.downloadInto(path);
                artifactSource.get(artifact).updateParams(params, artifact, path);
              } catch (IOException e) {
                errors.put(artifact.getName(), e);
              }
            }
          }));
      }
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (ExecutionException e) {
          // ignore, this should not happen.
        }
      }
    }
    writeErrors(errors);
  }

  @Override
  public void close() throws IOException {
    try {
      if (zipfs != null) {
        zipfs.close();
      }
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }

  private List<Artifact> collectDownloadableArtifacts() {
    List<Artifact> artifacts = new ArrayList<>();
    Iterator<ArtifactSource> iter = pendingSources.iterator();
    while (iter.hasNext()) {
      ArtifactSource source = iter.next();
      if (source.hasRequiredParams(params)) {
        for (Artifact artifact : source.getArtifacts(params)) {
          artifacts.add(artifact);
          artifactSource.put(artifact, source);
        }
        iter.remove();
      }
    }
    return artifacts;
  }

  private boolean writeErrors(Map<String, Exception> errors) {
    if (!errors.isEmpty()) {
      Path path = zipfs.getPath("ERRORS");
      OutputStream stream = null;
      try {
        stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
        Map<String, String> formattedError = new HashMap<>();
        for (Entry<String, Exception> entry : errors.entrySet()) {
          StringWriter writer = new StringWriter();
          entry.getValue().printStackTrace(new PrintWriter(writer));
          formattedError.put(entry.getKey(), writer.toString());
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        objectMapper.writeValue(stream, formattedError);
      } catch (IOException e) {
        // LOG error here?
        e.printStackTrace();
        return false;
      } finally {
        // We should not close a stream from ZipFileSystem, I have no clue why.
        // IOUtils.closeQuietly(stream);
      }
    }
    return true;
  }
}
