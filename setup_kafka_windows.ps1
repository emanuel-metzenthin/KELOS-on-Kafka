Remove-Item -path C:\tmp -recurse -ErrorAction Ignore

Start-Process ./bin/windows/zookeeper-server-start.bat config/zookeeper.properties

Start-Sleep -s 10

Start-Process ./bin/windows/kafka-server-start.bat config/server.properties

Start-Sleep -s 10

bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic data-input

bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic clusters

bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic clusters
