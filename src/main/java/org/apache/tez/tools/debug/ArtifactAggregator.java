package org.apache.tez.tools.debug;

import java.io.Closeable;
import java.io.File;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.tez.tools.debug.Params.Param;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class ArtifactAggregator implements Closeable {
  private final Configuration conf;
  private final Params params;
  private final CloseableHttpClient httpClient;
  private final Injector injector;
  private final FileSystem zipfs;
  private final List<ArtifactSource> pendingSources;

  public ArtifactAggregator(Configuration conf_, Params params_, String zipFilePath,
      List<ArtifactSourceType> sourceTypes) throws IOException {
    this.conf = conf_;
    this.params = new Params(params_);
    this.httpClient = HttpClients.createDefault();
    this.injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(HttpClient.class).toInstance(httpClient);
        binder.bind(Configuration.class).toInstance(conf);
        binder.bind(Params.class).toInstance(params);
      }
    });
    this.zipfs = createZipFS(zipFilePath);
    this.pendingSources = new ArrayList<>(sourceTypes.size());
    for (ArtifactSourceType sourceType : sourceTypes) {
      pendingSources.add(sourceType.getSource(injector));
    }
  }

  public void aggregate() {
    Map<String, Exception> errors = new HashMap<>();
    while (!pendingSources.isEmpty()) {
      List<Artifact> artifacts = collectDownloadableArtifacts();
      if (artifacts.isEmpty() && !pendingSources.isEmpty()) {
        // Artifacts is empty, but some sources are pending.
        // Can be because dagId was given, queryId could not be found, etc ...
        break;
      }
      for (Artifact artifact : artifacts) {
        Path path = zipfs.getPath(artifact.getName());
        try {
          artifact.downloadInto(path);
        } catch (IOException e) {
          errors.put(artifact.getName(), e);
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
      if (params.containsAll(source.getRequiredParams())) {
        artifacts.addAll(source.getArtifacts(params));
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

  private FileSystem createZipFS(String zipFilePath) throws IOException {
    Map<String, String> zip_properties = new HashMap<>();
    zip_properties.put("create", "true");
    zip_properties.put("encoding", "UTF-8");
    URI zip_disk = URI.create("jar:file:" + zipFilePath);
    FileSystem zipfs = FileSystems.newFileSystem(zip_disk, zip_properties);
    return zipfs;
  }

  private static void usage() {
    System.err.println(
        "Usage: download_aggregate <--dagId dagId | --queryId queryId> [--outputFile outputFile]");
    System.exit(1);
  }

  // dag_1494824006032_0001_1
  public static void main(String[] args) throws IOException {
    String dagId = null;
    String queryId = null;
    File outputFile = null;
    if (args.length % 2 != 0) {
      usage();
    }
    for (int i = 0; i < args.length; i += 2) {
      if (args[i].equals("--dagId")) {
        dagId = args[i + 1];
      } else if (args[i].equals("--queryId")) {
        queryId = args[i + 1];
      } else if (args[i].equals("--outputFile")) {
        outputFile = new File(args[i + 1]);
      } else {
        usage();
      }
    }
    if ((dagId == null && queryId == null) || (dagId != null && queryId != null)) {
      usage();
    }
    Params params = new Params();
    if (dagId != null) {
      params.setParam(Param.TEZ_DAG_ID, dagId);
    }
    if (queryId != null) {
      params.setParam(Param.HIVE_QUERY_ID, queryId);
    }
    if (outputFile == null) {
      outputFile = new File((dagId == null ? queryId : dagId) + ".zip");
    }
    if (outputFile.exists()) {
      System.err.println("File already exists: " + outputFile.getAbsolutePath());
      System.exit(1);
    }
    Configuration conf = new Configuration();
    // Add option to download only a subset of sources.
    List<ArtifactSourceType> sourceTypes = Arrays.asList(ArtifactSourceType.values());
    ArtifactAggregator aggregator = new ArtifactAggregator(
        conf, params, outputFile.getAbsolutePath(), sourceTypes);
    aggregator.aggregate();
    aggregator.close();
  }
}
