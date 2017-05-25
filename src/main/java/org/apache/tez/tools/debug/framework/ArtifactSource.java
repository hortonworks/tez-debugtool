package org.apache.tez.tools.debug.framework;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface ArtifactSource {
  boolean hasRequiredParams(Params params);
  List<Artifact> getArtifacts(Params params);
  void updateParams(Params params, Artifact artifact, Path path) throws IOException;
}
