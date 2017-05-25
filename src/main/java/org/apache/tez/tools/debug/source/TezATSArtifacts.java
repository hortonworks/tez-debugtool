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

public class TezATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;

  @Inject
  public TezATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    String dagId = params.getTezDagId();
    try {
      return ImmutableList.of(
          helper.getEntityArtifact("TEZ_DAG", "TEZ_DAG_ID", dagId),
          helper.getEntityArtifact("TEZ_DAG_EXTRAINFO", "TEZ_DAG_EXTRA_INFO", dagId),
          helper.getChildEntityArtifact("TEZ_VERTEX", "TEZ_VERTEX_ID", "TEZ_DAG_ID", dagId),
          helper.getChildEntityArtifact("TEZ_TASK", "TEZ_TASK_ID", "TEZ_DAG_ID", dagId),
          helper.getChildEntityArtifact("TEZ_TASK_ATTEMPT", "TEZ_TASK_ATTEMPT_ID", "TEZ_DAG_ID",
              dagId));
    } catch (URISyntaxException e) {
      // This should go back to user.
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    if (params.getHiveQueryId() != null && params.getTezDagId() != null) {
      return;
    }
    if (artifact.getName().equals("TEZ_DAG")) {
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
        JsonNode appId = other.get("applicationId");
        if (appId != null && appId.isTextual()) {
          params.setTezAmAppId(appId.asText());
        }
      }
      if (params.getHiveQueryId() == null) {
        JsonNode callerType = other.get("callerType");
        if (callerType != null && callerType.isTextual() &&
            callerType.asText().equals("HIVE_QUERY_ID")) {
          JsonNode callerId = other.get("callerId");
          if (callerId != null && callerId.isTextual()) {
            params.setHiveQueryId(callerId.asText());
          }
        }
      }
    }
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getTezDagId() != null;
  }
}
