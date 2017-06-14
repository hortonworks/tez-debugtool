package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.ContainerLogInfo;
import org.apache.tez.tools.debug.framework.Params.ContainerLogsInfo;

import com.google.inject.Inject;

public class TezTasksLogsListArtifacts implements ArtifactSource {
  private final AMArtifactsHelper helper;

  @Inject
  public TezTasksLogsListArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getAppType() != null && params.getAppType().equals("TEZ") &&
        params.getTezTaskLogs().isFinishedContainers();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getTezTaskLogs().getLogListArtifacts(helper, "TEZ_TASKS/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    List<ContainerLogsInfo> logsInfoList = helper.parseContainerLogs(path);
    if (logsInfoList != null) {
      for (ContainerLogsInfo logsInfo : logsInfoList) {
        filterLogs(logsInfo.containerLogInfo, params);
        params.getTezTaskLogs().addLog(
            logsInfo.nodeId, logsInfo.containerId, logsInfo.containerLogInfo);
      }
      // This is not correct, but we have no way to tell all the logs have downloaded
      params.getTezTaskLogs().finishLogs();
    }
  }

  private void filterLogs(List<ContainerLogInfo> containerLogInfo, Params params) {
    String syslogPrefix = "syslog_attempt_" + params.getTezDagId().substring(4) + "_";
    Iterator<ContainerLogInfo> iter = containerLogInfo.iterator();
    while (iter.hasNext()) {
      String fileName = iter.next().fileName;
      if (fileName.startsWith("syslog_attempt_") && !fileName.startsWith(syslogPrefix)) {
        iter.remove();
      }
    }
  }

}
