package org.apache.tez.tools.debug;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.tools.debug.framework.ArtifactAggregator;
import org.apache.tez.tools.debug.framework.ArtifactSourceType;
import org.apache.tez.tools.debug.framework.Params;

public class Main {
  public static void main(String[] args) {
    String dagId = null;
    String queryId = null;
    File outputFile = null;
    List<ArtifactSourceType> sourceTypes = new ArrayList<>();
    if (args.length % 2 != 0) {
      usage("Got extra arguments.");
    }
    for (int i = 0; i < args.length; i += 2) {
      if (args[i].equals("--dagId")) {
        dagId = args[i + 1];
      } else if (args[i].equals("--queryId")) {
        queryId = args[i + 1];
      } else if (args[i].equals("--outputFile")) {
        outputFile = new File(args[i + 1]);
      } else if (args[i].equals("--sourceType")) {
        try {
          sourceTypes.add(ArtifactSourceType.valueOf(args[i + 1]));
        } catch (IllegalArgumentException e) {
          usage("Invalid source type: " + args[ i + 1]);
        }
      } else {
        usage("Unknown option: " + args[i]);
      }
    }
    if ((dagId == null && queryId == null) || (dagId != null && queryId != null)) {
      usage("Specify either dagId or queryId.");
    }
    Params params = new Params();
    if (dagId != null) {
      params.setTezDagId(dagId);
    }
    if (queryId != null) {
      params.setHiveQueryId(queryId);
    }
    if (outputFile == null) {
      outputFile = new File((dagId == null ? queryId : dagId) + ".zip");
    }
    if (outputFile.exists()) {
      usage("File already exists: " + outputFile.getAbsolutePath());
    }

    // Add command line option to download only a subset of sources.
    if (sourceTypes.isEmpty()) {
      sourceTypes = Arrays.asList(ArtifactSourceType.values());
    }

    ExecutorService service = Executors.newFixedThreadPool(1);
    try (ArtifactAggregator aggregator = new ArtifactAggregator(new Configuration(), service,
        params, outputFile.getAbsolutePath(), sourceTypes)) {
      aggregator.aggregate();
    } catch (Exception e) {
      System.err.println("Error occured while trying to create aggregator: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      service.shutdownNow();
    }
  }

  private static void usage(String msg) {
    if (msg != null) {
      System.err.println(msg);
    }
    System.err.println(
        "Usage: download_aggregate <--dagId dagId | --queryId queryId> " +
        "[--sourceType <sourceType>]... [--outputFile outputFile]");
    System.exit(1);
  }
}
