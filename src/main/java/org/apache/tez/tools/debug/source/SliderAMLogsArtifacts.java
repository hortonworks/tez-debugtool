package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

import com.google.inject.Inject;

public class SliderAMLogsArtifacts implements ArtifactSource {

  private final AMArtifactsHelper helper;

  @Inject
  public SliderAMLogsArtifacts(AMArtifactsHelper helper) {
    this.helper = helper;
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return params.getSliderAmLogs().isFinishedLogs();
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return params.getSliderAmLogs().getLogArtifacts(helper, "SLIDER_AM/LOGS");
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
  }
}
