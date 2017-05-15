# tez-debugtool

Tool/Library to download artifacts for hive/tez projects.

BUILD:
* Build using mvn clean package.

RUNNING:
* mvn exec:java -Dexec.mainClass=org.apache.tez.tools.debug.ArtifactAggregator -Dexec.args="--dagId <dag_id>"
