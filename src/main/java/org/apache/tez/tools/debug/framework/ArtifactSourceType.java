package org.apache.tez.tools.debug.framework;

import org.apache.tez.tools.debug.source.DummyArtifacts;
import org.apache.tez.tools.debug.source.HiveATSArtifacts;
import org.apache.tez.tools.debug.source.LlapDeamonLogsArtifacts;
import org.apache.tez.tools.debug.source.LlapDeamonLogsListArtifacts;
import org.apache.tez.tools.debug.source.TezAMInfoArtifacts;
import org.apache.tez.tools.debug.source.TezAMLogsArtifacts;
import org.apache.tez.tools.debug.source.TezAMLogsListArtifacts;
import org.apache.tez.tools.debug.source.TezATSArtifacts;

import com.google.inject.Injector;

public enum ArtifactSourceType {
  TEZ_ATS(TezATSArtifacts.class),
  TEZ_AM_INFO(TezAMInfoArtifacts.class),
  TEZ_AM_LOG_INFO(TezAMLogsListArtifacts.class),
  TEZ_AM_LOGS(TezAMLogsArtifacts.class),
  TEZ_AM_JMX(DummyArtifacts.class),
  TEZ_AM_STACK(DummyArtifacts.class),
  LLAP_DEAMON_LOGS_INFO(LlapDeamonLogsListArtifacts.class),
  LLAP_DEAMON_LOGS(LlapDeamonLogsArtifacts.class),
  LLAP_DEAMON_JMX(DummyArtifacts.class),
  LLAP_DEAMON_STACK(DummyArtifacts.class),
  TEZ_CONFIG(DummyArtifacts.class),
  TEZ_HIVE2_CONFIG(DummyArtifacts.class),
  HIVE_ATS(HiveATSArtifacts.class),
  HIVE_CONFIG(DummyArtifacts.class),
  HIVE2_CONFIG(DummyArtifacts.class),
  HADOOP_CONFIG(DummyArtifacts.class),
  HIVESERVER2_LOG(DummyArtifacts.class);

  private final Class<? extends ArtifactSource> sourceClass;
  private ArtifactSourceType(Class<? extends ArtifactSource> sourceClass) {
    this.sourceClass = sourceClass;
  }

  public ArtifactSource getSource(Injector injector) {
    return injector.getInstance(sourceClass);
  }
}
