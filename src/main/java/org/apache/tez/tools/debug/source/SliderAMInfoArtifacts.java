package org.apache.tez.tools.debug.source;

import org.apache.tez.tools.debug.AMArtifactsHelper;
import org.apache.tez.tools.debug.framework.Params;
import org.apache.tez.tools.debug.framework.Params.AppLogs;

import com.google.inject.Inject;

public class SliderAMInfoArtifacts extends AMInfoArtifacts {

  @Inject
  public SliderAMInfoArtifacts(AMArtifactsHelper helper) {
    super(helper);
  }

  @Override
  public String getArtifactName() {
    return "SLIDER_AM/INFO";
  }

  @Override
  public String getAmId(Params params) {
    return params.getSliderAppId();
  }

  @Override
  public AppLogs getAMAppLogs(Params params) {
    return params.getSliderAmLogs();
  }
}
