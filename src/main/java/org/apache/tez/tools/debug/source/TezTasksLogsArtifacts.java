package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

import com.google.inject.Inject;

public class TezTasksLogsArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;

  @Inject
  public TezTasksLogsArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getAppType() != null && params.getAppType().equals("TEZ") &&
        params.getTezTaskLogs().isFinishedLogs();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getTezTaskLogs().getLogArtifacts(helper, "TEZ_TASKS/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
  }

}
