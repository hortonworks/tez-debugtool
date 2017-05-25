package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.tez.tools.debug.ATSArtifactHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

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
  public List<Artifact> getArtifacts(Params params) {
    try {
      return ImmutableList.of(helper.getEntityArtifact("HIVE_QUERY", "HIVE_QUERY_ID",
          params.getHiveQueryId()));
    } catch (URISyntaxException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    if (params.getTezDagId() != null && params.getTezAmAppId() != null) {
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
      if (params.getTezAmAppId() == null) {
        JsonNode appId = other.get("APP_ID");
        if (appId != null && appId.isTextual()) {
          params.setTezAmAppId(appId.asText());
        }
      }
      if (params.getTezDagId() == null) {
        JsonNode dagId = other.get("DAG_ID");
        if (dagId != null && dagId.isTextual()) {
          params.setTezDagId(dagId.asText());
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getHiveQueryId() != null;
  }
}
