./bin/flink run \
  --detached \
	--python usrlib/artifacts/python/prase_api.py \
	--jarfile ./lib/flink-sql-connector-kafka-3.2.0-1.19.jar

./bin/flink run \
      --detached \
      ./usrlib/artifacts/java/myartifact-0.1.jar