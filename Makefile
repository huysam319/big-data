# Default script (can be overridden)
SCRIPT ?= stream.py 

run:
	docker exec -u 0 spark-master mkdir -p /home/spark/.ivy2/cache && \
	docker exec -u 0 spark-master chown -R 185:185 /home/spark && \
	docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.0 /opt/scripts/$(SCRIPT)

copy:
	sudo docker cp ./scripts/stream.py spark-master:/opt/scripts/stream.py

list:
	sudo docker exec -u root -it spark-master ls -l /opt/scripts	

restart:
	rm -rf /tmp/spark_checkpoints/mimic_kafka && \
	rm -rf /tmp/spark_checkpoints/mimic_pg && \
	docker exec -it namenode hadoop fs -rm -r /tmp/spark_checkpoints/mimic_pg && \
	sudo docker restart spark-master

consumer:
	python ./scripts/consumer.py

producer:
	python ./scripts/producer.py

.PHONY: run list restart copy consumer producer
