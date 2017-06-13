package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.tez.tools.debug.HttpArtifact;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

import com.google.inject.Inject;

public class SliderInstanceStack implements ArtifactSource {

  private final HttpClient client;

  @Inject
  public SliderInstanceStack(HttpClient client) {
    this.client = client;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getSliderInstanceUrls() != null;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    List<Artifact> artifacts = new ArrayList<>();
    for (String url : params.getSliderInstanceUrls()) {
      try {
        URIBuilder builder = new URIBuilder(url);
        builder.setPath(builder.getPath() + "/stacks");
        artifacts.add(new HttpArtifact(client,
            "SLIDER_AM/" + builder.getHost() + ":" + builder.getPort() + "/stacks",
            builder.build().toString(), false));
      } catch (URISyntaxException e) {
        // Return this to user.
        e.printStackTrace();
      }
    }
    return artifacts;
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
  }
}
