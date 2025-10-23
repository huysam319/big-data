# Default script (can be overridden)
SCRIPT ?= main.scala

# Run Scala script in Spark container
run:
	docker exec spark-master /opt/spark/bin/spark-shell --master local[*] -i /opt/scripts/$(SCRIPT)

list:
	docker exec spark-master ls -la /opt/scripts/