package org.apache.tez.tools.debug;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tez.tools.debug.Params.Param;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class TezATSArtifacts implements ArtifactSource {

  private final ATSArtifactHelper helper;

  @Inject
  public TezATSArtifacts(ATSArtifactHelper helper) {
    this.helper = helper;
  }

  @Override
  public Set<Param> getRequiredParams() {
    return Collections.singleton(Param.TEZ_DAG_ID);
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    String dagId = params.getParam(Param.TEZ_DAG_ID);
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
}
