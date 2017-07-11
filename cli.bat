REM java -Dlog4j.debug=true -Dlog4j.filename=cli-log4j.properties -jar lib/ksql-cli-1.0-SNAPSHOT-standalone.jar local
REM java -cp .;lib/ksql-cli-1.0-SNAPSHOT-standalone.jar -Dlog4j.debug=true  io.confluent.ksql.Ksql local
java -cp .;C:\work\KKVStore\out\production\KKVStore;lib/kafka-clients-0.11.1.0-SNAPSHOT.jar;lib/kafka-streams-0.11.1.0-SNAPSHOT.jar;lib/ksql-cli-1.0-SNAPSHOT-standalone.jar -Dlog4j.debug=true  io.confluent.ksql.Ksql local

