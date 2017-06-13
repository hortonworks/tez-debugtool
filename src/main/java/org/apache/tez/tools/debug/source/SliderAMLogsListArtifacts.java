package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.ContainerLogsInfo;

import com.google.inject.Inject;

public class SliderAMLogsListArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;

  @Inject
  public SliderAMLogsListArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getSliderAmLogs().isFinishedContainers();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getSliderAmLogs().getLogListArtifacts(helper, "SLIDER_AM/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    List<ContainerLogsInfo> logsInfoList = helper.parseContainerLogs(path);
    if (logsInfoList != null) {
      for (ContainerLogsInfo logsInfo : logsInfoList) {
        // filterLogs(logsInfo.containerLogInfo, params);
        params.getSliderAmLogs().addLog(logsInfo.nodeId, logsInfo.containerId,
            logsInfo.containerLogInfo);
      }
      // This is not correct, but we have no way to tell all the logs have downloaded
      params.getSliderAmLogs().finishLogs();
    }
  }

}
