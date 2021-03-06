package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.ContainerLogInfo;
import org.apache.tez.tools.debug.framework.Params.ContainerLogsInfo;

import com.google.inject.Inject;

public class TezAMLogsListArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;

  @Inject
  public TezAMLogsListArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezAmLogs().isFinishedContainers();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getTezAmLogs().getLogListArtifacts(helper, "TEZ_AM/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    List<ContainerLogsInfo> logsInfoList = helper.parseContainerLogs(path);
    if (logsInfoList != null) {
      for (ContainerLogsInfo logsInfo : logsInfoList) {
        filterLogs(logsInfo.containerLogInfo, params.getTezDagId());
        params.getTezAmLogs().addLog(logsInfo.nodeId, logsInfo.containerId,
            logsInfo.containerLogInfo);
      }
      // This is not correct, but we have no way to tell all the logs have downloaded
      params.getTezAmLogs().finishLogs();
    }
  }

  private static Pattern dagPattern = Pattern.compile("dag_\\d+_\\d+_\\d+");
  private void filterLogs(List<ContainerLogInfo> logs, String dagId) {
    Iterator<ContainerLogInfo> iter = logs.iterator();
    while (iter.hasNext()) {
      Matcher matcher = dagPattern.matcher(iter.next().fileName);
      if (matcher.find() && !matcher.group().equals(dagId)) {
        iter.remove();
      }
    }
  }
}
