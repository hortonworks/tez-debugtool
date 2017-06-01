package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tez.tools.debug.ATSArtifactHelper;
import org.apache.tez.tools.debug.ATSArtifactHelper.ATSEvent;
import org.apache.tez.tools.debug.ATSArtifactHelper.ATSLog;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.AppLogs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class TezATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;
  private final ObjectMapper mapper;
  private final Pattern logsPattern;

  @Inject
  public TezATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    logsPattern = Pattern.compile(
        "^.*applicationhistory/containers/(.*?)/logs.*\\?nm.id=(.+:[\\d+]+).*$");
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    String dagId = params.getTezDagId();
    try {
      return ImmutableList.of(
          helper.getEntityArtifact("TEZ_DAG", "TEZ_DAG_ID", dagId),
          helper.getEntityArtifact("TEZ_DAG_EXTRAINFO", "TEZ_DAG_EXTRA_INFO", dagId),
          helper.getChildEntityArtifact("TEZ_VERTEX", "TEZ_VERTEX_ID", "TEZ_DAG_ID", dagId),
          helper.getChildEntityArtifact("TEZ_TASK", "TEZ_TASK_ID", "TEZ_DAG_ID", dagId),
          helper.getChildEntityArtifact("TEZ_TASK_ATTEMPT", "TEZ_TASK_ATTEMPT_ID", "TEZ_DAG_ID",
              dagId));
    } catch (URISyntaxException e) {
      // This should go back to user.
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    if (artifact.getName().equals("TEZ_DAG")) {
      extractDagData(params, path);
    }
    if (artifact.getName().equals("TEZ_TASK_ATTEMPT")) {
      extractTaskContainers(params, path);
    }
  }

  private void extractTaskContainers(Params params, Path path)
      throws IOException, JsonProcessingException {
    AppLogs appLogs = params.getTezAppLogs();
    if (appLogs.isFinishedLogs()) {
      return;
    }
    InputStream stream = Files.newInputStream(path);
    JsonNode node = mapper.readTree(stream);
    if (node == null || !node.isObject()) {
      return;
    }
    node = node.get("entities");
    if (node == null || !node.isArray()) {
      return;
    }
    for (int i = 0; i < node.size(); ++i) {
      String logsUrl = node.get(i).path("otherinfo").path("completedLogsURL").textValue();
      if (logsUrl == null) {
        continue;
      }
      Matcher matcher = logsPattern.matcher(logsUrl);
      if (matcher.matches()) {
        String containerId = matcher.group(1);
        String nodeId = matcher.group(2);
        appLogs.addContainer(nodeId, containerId);
      }
    }
    appLogs.finishContainers();
  }

  private void extractDagData(Params params, Path path) throws IOException {
    InputStream stream = Files.newInputStream(path);
    JsonNode node = mapper.readTree(stream);
    if (node == null) {
      return;
    }
    JsonNode other = node.get("otherinfo");
    if (other == null) {
      return;
    }
    // Get and update dag id/hive query id.
    if (params.getTezAmAppId() == null) {
      String appId = other.path("applicationId").textValue();
      if (appId != null) {
        params.setTezAmAppId(appId);
      }
    }
    if (params.getHiveQueryId() == null) {
      String callerType = other.path("callerType").textValue();
      String callerId = other.path("callerId").textValue();
      if (callerType != null && callerId != null && callerType.equals("HIVE_QUERY_ID")) {
        params.setHiveQueryId(callerId);
      }
    }
    ATSLog log = mapper.treeToValue(node, ATSLog.class);
    for (ATSEvent event : log.events) {
      if (event.eventtype != null) {
        if (event.eventtype.equals("DAG_SUBMITTED")) {
          params.updateStartTime(event.timestamp);
        } else if (event.eventtype.equals("DAG_FINISHED")) {
          params.updateEndTime(event.timestamp);
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() != null;
  }
}
