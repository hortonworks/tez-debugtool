package org.apache.tez.tools.debug;

import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.tez.tools.debug.framework.Artifact;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class ATSArtifactHelper {
  // This is a private field in yarn timeline client :-(.
  private static final String ATS_PATH_PREFIX = "/ws/v1/timeline/";

  private final HttpClient httpClient;
  private final String atsAddress;

  public static class ATSEvent {
    public long timestamp;
    public String eventtype;
    // ignored eventinfo
  }
  public static class ATSLog {
    public List<ATSEvent> events;
    public String entitytype;
    public String entity;
    public long starttime;
    // ignored domain, relatedentities, primaryfilters, otherinfo.
  }

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

  public Artifact getEntityArtifact(String name, String entityType, String entityId) {
    try {
      URIBuilder builder = new URIBuilder(atsAddress);
      builder.setPath(ATS_PATH_PREFIX + entityType + "/" + entityId);
      String url = builder.build().toString();
      return new HttpArtifact(httpClient, name, url, false);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid atsAddress: " + atsAddress, e);
    }
  }

  public Artifact getChildEntityArtifact(String name, String entityType, String rootEntityType,
      String rootEntityId) {
    try {
      URIBuilder builder = new URIBuilder(atsAddress);
      builder.setPath(ATS_PATH_PREFIX + entityType);
      builder.setParameter("primaryFilter", rootEntityType + ":" + rootEntityId);
      String url = builder.build().toString();
      return new HttpArtifact(httpClient, name, url, false);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid atsAddress: " + atsAddress, e);
    }
  }
}
