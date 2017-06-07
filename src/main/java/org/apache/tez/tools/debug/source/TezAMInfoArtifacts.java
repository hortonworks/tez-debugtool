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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

public class TezAMInfoArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;
  private final ObjectMapper mapper;

  @Inject
  public TezAMInfoArtifacts(AMArtifactsHelper helper, ObjectMapper mapper) {
    this.helper = helper;
    this.mapper = mapper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezAmAppId() != null;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.singletonList(
        helper.getAMInfoArtifact("TEZ_AM_INFO", params.getTezAmAppId()));
  }

  public static class AppAttempt {
    public int id;
    public long startTime;
    public long finishedTime;
    public String containerId;
    public String nodeId;
    public String appAttemptId;
  }

  @JsonRootName("appAttempts")
  public static class AMInfo {
    public List<AppAttempt> appAttempt;
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    if (params.getTezAmLogs().isFinishedContainers()) {
      return;
    }
    if (artifact.getName().equals("TEZ_AM_INFO")) {
      AMInfo amInfo = mapper.readValue(Files.newInputStream(path), AMInfo.class);
      if (amInfo != null && amInfo.appAttempt != null) {
        AppLogs amLogs = params.getTezAmLogs();
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
