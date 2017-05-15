package org.apache.tez.tools.debug;

import com.google.inject.Injector;

public enum ArtifactSourceType {
  TEZ_ATS(TezATSArtifacts.class),
  TEZ_AM_LOG(DummyArtifacts.class),
  TEZ_AM_JMX(DummyArtifacts.class),
  TEZ_AM_STACK(DummyArtifacts.class),
  LLAP_DEAMON_LOG(DummyArtifacts.class),
  LLAP_DEAMON_JMX(DummyArtifacts.class),
  LLAP_DEAMON_STACK(DummyArtifacts.class),
  TEZ_CONFIG(DummyArtifacts.class),
  TEZ_HIVE2_CONFIG(DummyArtifacts.class),
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
