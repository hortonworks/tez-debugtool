package org.apache.tez.tools.debug.source;

import java.io.IOException;
import java.io.PushbackReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tez.tools.debug.framework.Artifact;
import org.apache.tez.tools.debug.framework.ArtifactSource;
import org.apache.tez.tools.debug.framework.Params;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

public class SliderStatusArtifacts implements ArtifactSource {

  private final ObjectMapper mapper;

  @Inject
  public SliderStatusArtifacts() {
    this.mapper = new ObjectMapper();
  }

  @Override
  public boolean hasRequiredParams(Params params) {
    return true;
  }

  @Override
  public List<Artifact> getArtifacts(Params params) {
    return Collections.<Artifact>singletonList(new Artifact() {
      @Override
      public boolean isTemp() {
        return false;
      }

      @Override
      public String getName() {
        return "SLIDER_AM/STATUS";
      }

      @Override
      public void downloadInto(Path path) throws IOException {
        Process process;
        try {
          // Is the name always llap0? How do we get the name?
          // Is there a way to get hive path from config?
          process = new ProcessBuilder("hive", "--service", "llapstatus", "--name", "llap0")
              .start();
        } catch (IOException e) {
          throw new IOException("Error execution hive --service llapstatus --name llap0, "
              + "please check your path for hive and permissions for the user.", e);
        }
        try {
          Files.copy(process.getInputStream(), path);
        } finally {
          process.destroy();
        }
        if (process.isAlive()) {
          // Warn the user that we have process leak.
        }
      }
    });
  }

  @Override
  public void updateParams(Params params, Artifact artifact, Path path) throws IOException {
    PushbackReader reader = new PushbackReader(Files.newBufferedReader(path));
    for (;;) {
      int ch = reader.read();
      if (ch < 0) {
        reader.close();
        return;
      }
      if (ch == '{') {
        reader.unread(ch);
        break;
      }
    }
    JsonNode tree = mapper.readTree(reader);
    if (tree == null) {
      return;
    }
    String sliderAppId = tree.path("amInfo").path("appId").textValue();
    if (sliderAppId != null) {
      params.setSliderAppId(sliderAppId);
    }
    JsonNode instances = tree.path("runningInstances");
    if (instances.isArray()) {
      Set<String> inst = new HashSet<>();
      try {
      for (int i = 0; i < instances.size(); ++i) {
        String nodeUrl = instances.path(i).path("webUrl").textValue();
        System.out.println("****** URL: " + instances.get(i));
        if (nodeUrl != null) {
          inst.add(nodeUrl);
        }
      }
      } catch(Throwable e) {e.printStackTrace();}
      System.out.println("***** Done with looping.");
      params.setSliderInstanceUrls(inst);
    }
  }
}
