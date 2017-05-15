package org.apache.tez.tools.debug;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.tez.tools.debug.Params.Param;

import com.google.common.collect.Lists;
import com.google.inject.Inject;

public class TezATSArtifacts implements ArtifactSource {

  private final Configuration conf;
  private final HttpClient httpClient;

  @Inject
  public TezATSArtifacts(Configuration conf, HttpClient httpClient) {
    this.conf = conf;
    this.httpClient = httpClient;
  }

  @Override
  public Set<Param> getRequiredParams() {
    return Collections.singleton(Param.TEZ_DAG_ID);
  }

  @Override
  public Set<Param> getProvidedParams() {
    return Collections.emptySet();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    // TODO(hjp): Extract from configuration.
    String atsHost = "localhost";
    int atsPort = 8188;
    String atsPathPrefix = "/ws/v1/timeline/";
    String dagId = params.getParam(Param.TEZ_DAG_ID);

    URIBuilder builder = new URIBuilder();
    builder.setScheme("http");
    builder.setHost(atsHost);
    builder.setPort(atsPort);

    try {
      builder.setPath(atsPathPrefix + "TEZ_DAG_ID/" + dagId);
      String dagUri = builder.build().toString();

      builder.setPath(atsPathPrefix + "TEZ_DAG_EXTRA_INFO/" + dagId);
      String dagExtraInfoUri = builder.build().toString();

      builder.setParameter("primaryFilter", "TEZ_DAG_ID:" + dagId);

      builder.setPath(atsPathPrefix + "TEZ_VERTEX_ID/");
      String vertexUri = builder.build().toString();

      builder.setPath(atsPathPrefix + "TEZ_TASK_ID/");
      String taskUri = builder.build().toString();

      builder.setPath(atsPathPrefix + "TEZ_TASK_ATTEMPT_ID/");
      String taskAttemptUri = builder.build().toString();

      return Lists.<Artifact>newArrayList(
          new HttpArtifact(httpClient, "TEZ_DAG", dagUri),
          new HttpArtifact(httpClient, "TEZ_DAG_EXTRAINFO", dagExtraInfoUri),
          new HttpArtifact(httpClient, "TEZ_VERTEX", vertexUri),
          new HttpArtifact(httpClient, "TEZ_TASK", taskUri),
          new HttpArtifact(httpClient, "TEZ_TASK_ATTEMPT", taskAttemptUri));
    } catch (URISyntaxException e) {
      // This should go back to user.
      e.printStackTrace();
      return null;
    }
  }
}
