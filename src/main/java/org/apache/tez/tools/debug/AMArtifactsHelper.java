package org.apache.tez.tools.debug;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.HttpClient;
import org.apache.tez.tools.debug.framework.Artifact;

import com.google.inject.Inject;

public class AMArtifactsHelper {

  private static final String RM_WS_PREFIX = "/ws/v1/cluster";
  private static final String AHS_WS_PREFIX = "/ws/v1/applicationhistory";

  private final HttpClient httpClient;
  private final String rmAddress;
  private final String ahsAddress;

  @Inject
  public AMArtifactsHelper(Configuration conf, HttpClient httpClient) {
    this.httpClient = httpClient;
    if (YarnConfiguration.useHttps(conf)) {
      rmAddress = "https://" + conf.get(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS);
      ahsAddress = "https://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      rmAddress = "http://" + conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
      ahsAddress = "http://" + conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
  }

  public Artifact getAMInfoArtifact(String name, String appId) {
    String attemptsUrl = rmAddress + RM_WS_PREFIX + "/apps/" + appId + "/appattempts";
    return new HttpArtifact(httpClient, name, attemptsUrl, true);
  }

  public Artifact getLogListArtifact(String name, String containerId, String nodeId) {
    String logsListUrl = ahsAddress + AHS_WS_PREFIX + "/containers/" + containerId + "/logs";
    if (nodeId != null) {
      logsListUrl += "?nm.id=" + nodeId;
    }
    return new HttpArtifact(httpClient, name, logsListUrl, true);
  }

  public Artifact getLogArtifact(String name, String containerId, String logFile, String nodeId) {
    String logUrl = ahsAddress + AHS_WS_PREFIX + "/containers/" + containerId + "/logs/" + logFile;
    if (nodeId != null) {
      logUrl += "?nm.id=" + nodeId;
    }
    return new HttpArtifact(httpClient, name, logUrl, false);
  }
}
