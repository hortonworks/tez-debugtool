package org.apache.tez.tools.debug;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tez.tools.debug.Params.Param;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class HiveATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;

  @Inject
  public HiveATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
  }

  @Override
  public Set<Param> getRequiredParams() {
    return Collections.singleton(Param.HIVE_QUERY_ID);
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    try {
      return ImmutableList.of(helper.getEntityArtifact("HIVE_QUERY", "HIVE_QUERY_ID",
          params.getParam(Param.HIVE_QUERY_ID)));
    } catch (URISyntaxException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void updateParams(Params param, Artifact artifact, Path path) throws IOException {
    if (param.getParam(Param.TEZ_DAG_ID) != null &&
        param.getParam(Param.TEZ_APP_ID) != null) {
      return;
    }
    if (artifact.getName().equals("HIVE_QUERY")) {
      InputStream stream = Files.newInputStream(path);
      JsonNode node = new ObjectMapper().readTree(stream);
      if (node == null) {
        return;
      }
      JsonNode other = node.get("otherinfo");
      if (other == null) {
        return;
      }
      // Get and update dag id/hive query id.
      if (param.getParam(Param.TEZ_APP_ID) == null) {
        JsonNode appId = other.get("APP_ID");
        if (appId != null && appId.isTextual()) {
          param.setParam(Param.TEZ_APP_ID, appId.asText());
        }
      }
      if (param.getParam(Param.TEZ_DAG_ID) == null) {
        JsonNode dagId = other.get("DAG_ID");
        if (dagId != null && dagId.isTextual()) {
          param.setParam(Param.TEZ_DAG_ID, dagId.asText());
        }
      }
    }
  }
}
