package org.apache.tez.tools.debug.framework;

import java.io.IOException;
import java.nio.file.Path;

public interface Artifact {
  String getName();
  void downloadInto(Path path) throws IOException;
}
