package org.apache.tez.tools.debug;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.client.HttpClient;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.Params.ContainerLogsInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
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
    return new HttpArtifact(httpClient, name, logsListUrl, false);
  }

  public Artifact getLogArtifact(String name, String containerId, String logFile, String nodeId) {
    String logUrl = ahsAddress + AHS_WS_PREFIX + "/containers/" + containerId + "/logs/" + logFile;
    if (nodeId != null) {
      logUrl += "?nm.id=" + nodeId;
    }
    return new HttpArtifact(httpClient, name, logUrl, false);
  }

  public List<ContainerLogsInfo> parseContainerLogs(Path path)
      throws IOException {
    TypeReference<List<ContainerLogsInfo>> typeRef = new TypeReference<List<ContainerLogsInfo>>(){};
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
      ObjectReader reader = mapper.reader(typeRef).withRootName("containerLogsInfo");
      return reader.readValue(Files.newInputStream(path));
    } catch (JsonProcessingException e) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
      return mapper.readValue(Files.newInputStream(path), typeRef);
    }
  }
}
