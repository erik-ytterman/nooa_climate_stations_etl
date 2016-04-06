yarn jar ./target/NooaClimateStationETL-1.0-SNAPSHOT.jar \
    /user/cloudera/climate-2015-12-15/jsondata/stations/ghcnd-stations.jsonl \
    /user/cloudera/climate-2015-12-15/outdata/stations \
    /user/cloudera/climate-2015-12-15/schemas/ghcnd-stations.avsc
