package org.apache.tez.tools.debug.framework;

import java.util.List;

public class Params {
  // Tez information.
  private String tezDagId;
  private String tezAmAppId;
  private String tezAmContainerId;
  private List<String> tezAmContainerLogs;

  // Hive information.
  private String hiveQueryId;

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

  public String getTezAmContainerId() {
    return tezAmContainerId;
  }

  public void setTezAmContainerId(String tezAmContainerId) {
    this.tezAmContainerId = tezAmContainerId;
  }

  public List<String> getTezAmContainerLogs() {
    return tezAmContainerLogs;
  }

  public void setTezAmContainerLogs(List<String> tezAmContainerLogs) {
    this.tezAmContainerLogs = tezAmContainerLogs;
  }

  public String getHiveQueryId() {
    return hiveQueryId;
  }

  public void setHiveQueryId(String hiveQueryId) {
    this.hiveQueryId = hiveQueryId;
  }
}
