package org.apache.tez.tools.debug.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tez.tools.debug.AMArtifactsHelper;

public class Params {
  // Tez information.
  private String tezDagId;
  private String tezAmAppId;

  private final AppLogs tezAmLogs = new AppLogs();
  private final AppLogs tezTaskLogs = new AppLogs();

  // Slider AM info.
  private String sliderAppId;
  private final AppLogs sliderAmLogs = new AppLogs();
  private Set<String> sliderInstanceUrls;

  // Hive information.
  private String hiveQueryId;

  // Start and End time of query/dag.
  private long startTime = 0;
  private long endTime = Long.MAX_VALUE;

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
          artifacts.add(helper.getLogListArtifact(name + "/" + containerId + ".logs.json",
              containerId, nodeId));
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
            artifacts.add(helper.getLogArtifact(name + "/" + containerId + "/" + log.fileName,
                containerId, log.fileName, nodeId));
          }
        }
      }
      return artifacts;
    }
  }

  public static class ContainerLogInfo {
    public String fileName;
    public long fileSize;
    public String lastModifiedTime;
  }

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

  public AppLogs getTezTaskLogs() {
    return tezTaskLogs;
  }

  public String getSliderAppId() {
    return sliderAppId;
  }

  public void setSliderAppId(String sliderAppId) {
    this.sliderAppId = sliderAppId;
  }

  public AppLogs getSliderAmLogs() {
    return sliderAmLogs;
  }

  public Set<String> getSliderInstanceUrls() {
    return sliderInstanceUrls;
  }

  public void setSliderInstanceUrls(Set<String> sliderInstanceUrls) {
    this.sliderInstanceUrls = sliderInstanceUrls;
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
    if (this.endTime == Long.MAX_VALUE || endTime > this.endTime) {
      this.endTime = endTime;
    }
  }

  public boolean shouldIncludeArtifact(long startTime, long endTime) {
    if (endTime == 0) {
      endTime = Long.MAX_VALUE;
    }
    // overlap is true if one of them started when other was running.
    return (this.startTime <= startTime && startTime <= this.endTime) ||
        (startTime <= this.startTime && this.startTime <= endTime);
  }
}
