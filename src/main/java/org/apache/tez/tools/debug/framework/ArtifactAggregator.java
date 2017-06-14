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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class ArtifactAggregator implements AutoCloseable {
  private final ExecutorService service;
  private final Params params;
  private final CloseableHttpClient httpClient;
  private final FileSystem zipfs;
  private final List<ArtifactSource> pendingSources;
  private final Map<Artifact, ArtifactSource> artifactSource;

  public ArtifactAggregator(final Configuration conf, ExecutorService service, Params params_,
      String zipFilePath, List<ArtifactSourceType> sourceTypes) throws IOException {
    this.service = service;
    this.params = params_;
    this.httpClient = HttpClients.createDefault();
    this.artifactSource = new HashMap<>();
    this.zipfs = FileSystems.newFileSystem(URI.create("jar:file:" + zipFilePath),
        ImmutableMap.of("create", "true", "encoding", "UTF-8"));

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(HttpClient.class).toInstance(httpClient);
        binder.bind(Configuration.class).toInstance(conf);
        binder.bind(Params.class).toInstance(params);
      }
    });
    this.pendingSources = new ArrayList<>(sourceTypes.size());
    for (ArtifactSourceType sourceType : sourceTypes) {
      pendingSources.add(sourceType.getSource(injector));
    }
  }

  public void aggregate() throws IOException {
    final Map<String, Throwable> errors = new HashMap<>();
    while (!pendingSources.isEmpty()) {
      List<Artifact> artifacts = collectDownloadableArtifacts();
      if (artifacts.isEmpty() && !pendingSources.isEmpty()) {
        // Artifacts is empty, but some sources are pending.
        // Can be because dagId was given, queryId could not be found, etc ...
        break;
      }
      List<Future<?>> futures = new ArrayList<>(artifacts.size());
      for (final Artifact artifact : artifacts) {
        final Path path = getArtifactPath(artifact.getName());
        futures.add(service.submit(new Runnable() {
          public void run() {
            try {
              artifact.downloadInto(path);
              artifactSource.get(artifact).updateParams(params, artifact, path);
            } catch (Throwable t) {
              errors.put(artifact.getName(), t);
            } finally {
              if (artifact.isTemp()) {
                try {
                  Files.delete(path);
                } catch (IOException ignore) {
                }
              }
            }
          }
        }));
      }
      // Its important that we wait for all futures in this stage, there are some cases where all
      // downloads/updates of one stage should finish before we start the next stage.
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (ExecutionException e) {
          // ignore, this should not happen, we catch all throwable and serialize into error
        }
      }
    }
    writeErrors(errors);
  }

  private Path getArtifactPath(String artifactName) throws IOException {
    final Path path = zipfs.getPath("/", artifactName.split("/"));
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    return path;
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

  private void writeErrors(Map<String, Throwable> errors) throws IOException {
    if (errors.isEmpty()) {
      return;
    }
    Path path = zipfs.getPath("ERRORS");
    OutputStream stream = null;
    try {
      stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(Include.NON_NULL);
      JsonGenerator generator = objectMapper.getFactory().createGenerator(stream);
      generator.writeStartObject();
      for (Entry<String, Throwable> entry : errors.entrySet()) {
        StringWriter writer = new StringWriter();
        entry.getValue().printStackTrace(new PrintWriter(writer));
        generator.writeStringField(entry.getKey(), writer.toString());
      }
      generator.writeEndObject();
      generator.close();
    } finally {
      // We should not close a stream from ZipFileSystem, I have no clue why.
      // IOUtils.closeQuietly(stream);
    }
  }
}
