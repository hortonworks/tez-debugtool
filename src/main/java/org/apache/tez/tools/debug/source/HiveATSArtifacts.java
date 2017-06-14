package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.tez.tools.debug.ATSArtifactHelper;
import org.apache.tez.tools.debug.ATSArtifactHelper.ATSEvent;
import org.apache.tez.tools.debug.ATSArtifactHelper.ATSLog;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class HiveATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;
  private final ObjectMapper mapper;

  @Inject
  public HiveATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return ImmutableList.of(helper.getEntityArtifact("HIVE_QUERY", "HIVE_QUERY_ID",
        params.getHiveQueryId()));
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    if (artifact.getName().equals("HIVE_QUERY")) {
      InputStream stream = Files.newInputStream(path);
      JsonNode node = mapper.readTree(stream);
      if (node == null) {
        return;
      }
      if (params.getAppType() == null) {
        params.setAppType(node.path("primaryfilters").path("executionmode").path(0).textValue());
      }
      JsonNode other = node.get("otherinfo");
      if (other == null) {
        return;
      }
      // Get and update dag id/hive query id.
      if (params.getTezAmAppId() == null) {
        String appId = other.path("APP_ID").textValue();
        if (appId != null) {
          params.setTezAmAppId(appId);
        }
      }
      if (params.getTezDagId() == null) {
        String dagId = other.path("DAG_ID").textValue();
        if (dagId != null) {
          params.setTezDagId(dagId);
        }
      }
      ATSLog log = mapper.treeToValue(node, ATSLog.class);
      for (ATSEvent event : log.events) {
        if (event.eventtype != null) {
          if (event.eventtype.equals("QUERY_SUBMITTED")) {
            params.updateStartTime(event.timestamp);
          } else if (event.eventtype.equals("QUERY_COMPLETED")) {
            params.updateEndTime(event.timestamp);
          }
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getHiveQueryId() != null;
  }
}
