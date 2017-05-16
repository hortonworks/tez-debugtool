package org.apache.tez.tools.debug;

import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class ATSArtifactHelper {
  // This is a private field in yarn timeline client :-(.
  private static final String ATS_PATH_PREFIX = "/ws/v1/timeline/";

  private final HttpClient httpClient;
  private final String atsAddress;

  @Inject
  public ATSArtifactHelper(Configuration conf, HttpClient httpClient) {
    this.httpClient = httpClient;
    if (YarnConfiguration.useHttps(conf)) {
      atsAddress = "https://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      atsAddress = "http://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
  }

  public Artifact getEntityArtifact(String name, String entityType, String entityId)
      throws URISyntaxException {
    URIBuilder builder = new URIBuilder(atsAddress);
    builder.setPath(ATS_PATH_PREFIX + entityType + "/" + entityId);
    String url = builder.build().toString();
    return new HttpArtifact(httpClient, name, url);
  }

  public Artifact getChildEntityArtifact(String name, String entityType, String rootEntityType,
      String rootEntityId) throws URISyntaxException {
    URIBuilder builder = new URIBuilder(atsAddress);
    builder.setPath(ATS_PATH_PREFIX + entityType);
    builder.setParameter("primaryFilter", rootEntityType + ":" + rootEntityId);
    String url = builder.build().toString();
    return new HttpArtifact(httpClient, name, url);
  }
}
