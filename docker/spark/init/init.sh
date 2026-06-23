#!/bin/bash

# Load environment variables if needed
if [ -f "/opt/spark/conf/spark-env.sh" ]; then
    . "/opt/spark/conf/spark-env.sh"
fi

if [ "$SPARK_MODE" == "master" ]; then
    echo "Starting Spark Master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --host $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT
elif [ "$SPARK_MODE" == "worker" ]; then
    echo "Starting Spark Worker..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port $SPARK_WORKER_WEBUI_PORT \
        $SPARK_MASTER_URL
else
    echo "Unknown SPARK_MODE: $SPARK_MODE. Running custom command: $@"
    exec "$@"
fi
