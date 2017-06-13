package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.AppLogs;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AMInfoArtifacts implements ArtifactSource {

  protected final AMArtifactsHelper helper;
  protected final ObjectMapper mapper;

  public AMInfoArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
  }

  public abstract String getArtifactName();

  public abstract String getAmId(Params params);

  public abstract AppLogs getAMAppLogs(Params params);

  @Override
  public boolean hasRequiredParams(Params params) {
    return getAmId(params) != null;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.singletonList(
        helper.getAMInfoArtifact(getArtifactName(), getAmId(params)));
  }

  @JsonRootName("appAttempts")
  public static class AMInfo {
    public List<AppAttempt> appAttempt;
  }

  public static class AppAttempt {
    public int id;
    public long startTime;
    public long finishedTime;
    public String containerId;
    public String nodeId;
    public String appAttemptId;
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    AppLogs amLogs = getAMAppLogs(params);
    if (amLogs.isFinishedContainers()) {
      return;
    }
    if (artifact.getName().equals(getArtifactName())) {
      AMInfo amInfo = mapper.readValue(Files.newInputStream(path), AMInfo.class);
      if (amInfo != null && amInfo.appAttempt != null) {
        for (AppAttempt attempt: amInfo.appAttempt) {
          if (params.shouldIncludeArtifact(attempt.startTime, attempt.finishedTime)) {
            amLogs.addContainer(attempt.nodeId, attempt.containerId);
          }
        }
        amLogs.finishContainers();
      }
    }
  }
}