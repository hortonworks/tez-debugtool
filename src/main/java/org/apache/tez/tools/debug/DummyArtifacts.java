package org.apache.tez.tools.debug;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.tez.tools.debug.Params.Param;

public class DummyArtifacts implements ArtifactSource {

  @Override
  public Set<Param> getRequiredParams() {
    return Collections.emptySet();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.emptyList();
  }
}
