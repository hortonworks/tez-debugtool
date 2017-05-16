package org.apache.tez.tools.debug;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.apache.tez.tools.debug.Params.Param;

public interface ArtifactSource {
  Set<Param> getRequiredParams();
  List<Artifact> getArtifacts(Params params);
  void updateParams(Params param, Artifact artifact, Path path) throws IOException;
}
