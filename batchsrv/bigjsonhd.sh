hadoop fs -rm -R test/output/mr2
hadoop jar ./target/batchsrv-0.0.1-SNAPSHOT.jar com.visa.json.parser.IOCSimpleParser -libjars ./json-simple-1.1.1.jar test/input/json2 test/output/mr2
