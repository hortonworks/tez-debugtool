package org.apache.tez.tools.debug;

import java.util.EnumMap;
import java.util.Set;

public class Params {
  public enum Param {
    TEZ_DAG_ID,
    HIVE_QUERY_ID,
    TEZ_APP_ID;
  }

  private final EnumMap<Param, String> params = new EnumMap<>(Param.class);

  public Params() {}

  public Params(Params params) {
    this.params.putAll(params.params);
  }

  public String getParam(Param key) {
    return params.get(key);
  }

  public boolean setParam(Param key, String value) {
    if (!params.containsKey(key)) {
      params.put(key, value);
      return true;
    } else {
      return params.get(key).equals(value);
    }
  }

  public boolean containsAll(Set<Param> required) {
    return params.keySet().containsAll(required);
  }
}
