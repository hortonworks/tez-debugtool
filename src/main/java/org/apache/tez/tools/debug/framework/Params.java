package org.apache.tez.tools.debug.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tez.tools.debug.AMArtifactsHelper;

import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * This class will be access from multiple threads ensure that its thread safe.
 */
public class Params {
  // Tez information.
  private String tezDagId;
  private String tezAmAppId;

  private final AppLogs tezAmLogs = new AppLogs();
  private final AppLogs tezAppLogs = new AppLogs();

  // Hive information.
  private String hiveQueryId;

  private long startTime;
  private long endTime;

  public static class AppLogs {
    // Node -> Container -> log
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<ContainerLogInfo>>>
        appLogs = new ConcurrentHashMap<>();

    private boolean finishedContainers;
    private boolean finishedLogs;

    public void addLog(String nodeId, String containerId, List<ContainerLogInfo> logs)  {
      if (!appLogs.containsKey(nodeId)) {
        appLogs.putIfAbsent(nodeId, new ConcurrentHashMap<String, List<ContainerLogInfo>>());
      }
      System.out.println("Adding: " + nodeId + ": " + containerId);
      appLogs.get(nodeId).put(containerId, logs);
    }

    public void addContainer(String nodeId, String containerId) {
      addLog(nodeId, containerId, Collections.<ContainerLogInfo>emptyList());
    }

    public boolean isFinishedContainers() {
      return finishedContainers;
    }

    public void finishContainers() {
      this.finishedContainers = true;
    }

    public boolean isFinishedLogs() {
      return finishedLogs;
    }

    public void finishLogs() {
      this.finishedLogs = true;
    }

    public List<Artifact> getLogListArtifacts(AMArtifactsHelper helper, String name) {
      List<Artifact> artifacts = new ArrayList<>();
      for (Entry<String, ConcurrentHashMap<String, List<ContainerLogInfo>>> entry :
          appLogs.entrySet()) {
        String nodeId = entry.getKey();
        for (String containerId : entry.getValue().keySet()) {
          artifacts.add(helper.getLogListArtifact(name + "/" + containerId, containerId, nodeId));
        }
      }
      return artifacts;
    }

    public List<Artifact> getLogArtifacts(AMArtifactsHelper helper, String name) {
      List<Artifact> artifacts = new ArrayList<>();
      for (Entry<String, ConcurrentHashMap<String, List<ContainerLogInfo>>> entry :
          appLogs.entrySet()) {
        String nodeId = entry.getKey();
        for (Entry<String, List<ContainerLogInfo>> e : entry.getValue().entrySet()) {
          String containerId = e.getKey();
          for (ContainerLogInfo log: e.getValue()) {
            System.out.println("Request log: " + log.fileName);
            artifacts.add(helper.getLogArtifact(name + "/" + containerId + "/" + log.fileName,
                containerId, log.fileName, nodeId));
          }
        }
      }
      return artifacts;
    }
  }

  public static class AppAttempt {
    public int id;
    public long startTime;
    public long finishedTime;
    public String containerId;
    public String nodeId;
    public String appAttemptId;
  }

  public static class ContainerLogInfo {
    public String fileName;
    public long fileSize;
    public String lastModifiedTime;
  }

  @JsonRootName("containerLogsInfo")
  public static class ContainerLogsInfo {
    public List<ContainerLogInfo> containerLogInfo;
    public String containerId;
    public String nodeId;
  }

  public String getTezDagId() {
    return tezDagId;
  }

  public void setTezDagId(String tezDagId) {
    this.tezDagId = tezDagId;
  }

  public String getTezAmAppId() {
    return tezAmAppId;
  }

  public void setTezAmAppId(String tezAmAppId) {
    this.tezAmAppId = tezAmAppId;
  }


  public AppLogs getTezAmLogs() {
    return tezAmLogs;
  }

  public AppLogs getTezAppLogs() {
    return tezAppLogs;
  }

  public String getHiveQueryId() {
    return hiveQueryId;
  }

  public void setHiveQueryId(String hiveQueryId) {
    this.hiveQueryId = hiveQueryId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void updateStartTime(long startTime) {
    if (this.startTime == 0 || startTime < this.startTime) {
      this.startTime = startTime;
    }
  }

  public long getEndTime() {
    return endTime;
  }

  public void updateEndTime(long endTime) {
    if (this.endTime == 0 || endTime > this.endTime) {
      this.endTime = endTime;
    }
  }
}
