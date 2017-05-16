package org.apache.tez.tools.debug;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

public class HttpArtifact implements Artifact {

  private final HttpClient client;
  private final String name;
  private final String url;

  public HttpArtifact(HttpClient client, String name, String url) {
    this.client = client;
    this.name = name;
    this.url = url;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void downloadInto(Path path) throws IOException {
    // Try to use nio to transfer the streaming data from http into the outputstream of the path.
    // This would use about 3 buffers in place of one.
    HttpGet httpGet = new HttpGet(url);
    HttpResponse response = client.execute(httpGet);
    InputStream entityStream = null;
    try {
      entityStream = response.getEntity().getContent();
      Files.copy(entityStream, path);
    } finally {
      if (entityStream != null) {
        entityStream.close();
      }
    }
  }

  @Override
  public String toString() {
    return "HttpArtifact[Name: " + name + ", URL: " + url + "]";
  }
}
