package org.apache.tez.tools.debug.source;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

public class DummyArtifacts implements ArtifactSource {

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.emptyList();
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) {
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return false;
  }
}
