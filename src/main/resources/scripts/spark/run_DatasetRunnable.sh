#!/bin/bash

Usage="Example usage: .../scripts/run_DatasetRunnable.sh \"
--patientTable db.patients
--outputTable db.patients_selected
\""

if [[ $# -ne 1 ]]; then
  echo Usage
  exit 128
fi

export INPUT_PARAMETERS=$1

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
APP_PROPERTY_FILE_NAME="application.properties"
APP_PROPERTY_FILE="$DIR/${APP_PROPERTY_FILE_NAME}"
CLASS_NAME_TO_LOAD="DatasetRunnable"

SPARK_LOG="./run_DatasetRunnable_"$(date '+%Y%m%d%H%M%S')".log"
eval "$(sed -r 's/^([^=]*)=(.*)/export \1=\"\2\";/' ${APP_PROPERTY_FILE})"

echo "Getting a kerberos ticket for user"
kinit -kt "${KEYTAB_PATH}" -V "${KEYTAB_PRINCIPAL}"

spark2-submit \
  --class "com.github.blaval.scalaspark.runnable.${CLASS_NAME_TO_LOAD}" \
  --master yarn \
  --deploy-mode cluster \
  --queue ${QUEUE_NAME} \
  --executor-memory 8G \
  --executor-cores 4 \
  --driver-memory 6G \
  --files "${APP_PROPERTY_FILE},${LOGGER_PROPERTY_FILE}" \
  --keytab "${KEYTAB_PATH}" \
  --principal "${KEYTAB_PRINCIPAL}" \
  --jars ${LOGGER_JARS} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=6 \
  --conf spark.sql.shuffle.partitions=24 \
  --conf spark.sql.autoBroadcastJoinThreshold=100000000 \
  --conf spark.yarn.maxAppAttempts=3 \
  --conf spark.executor.memoryOverhead=4G \
  --conf spark.driver.extraClassPath="${SPARK_JARS}" \
  --conf spark.executor.extraClassPath="${SPARK_JARS}" \
  --conf spark.driver.extraJavaOptions="${SPARK_EXTRA_JAVA_OPTIONS}" \
  --conf spark.executor.extraJavaOptions="${SPARK_EXTRA_JAVA_OPTIONS}" \
  --conf spark.driver.cores=4 \
  --conf spark.driver.maxResultSize=6G \
  --conf spark.shuffle.sasl.timeout=${SPARK_SHUFFLE_SASL_TIMEOUT} \
  --conf spark.network.timeout=${SPARK_NETWORK_TIMEOUT} \
  --conf spark.reducer.maxBlocksInFlightPerAddress=64 \
  --conf spark.reducer.maxReqsInFlight=192 \
  --conf spark.port.maxRetries=300 \
  --conf spark.kryoserializer.buffer.max=1024 \
  --conf spark.shuffle.memoryFraction=0.6 \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.cleaner.referenceTracking=true \
  --conf spark.cleaner.referenceTracking.cleanCheckpoints=true \
  "${JAR_NAME}" ${INPUT_PARAMETERS} 2>&1 | tee -a "${SPARK_LOG}"

FINAL_STATUS=`grep "final status:" < "${SPARK_LOG}" | cut -d ":" -f2|tail -1`
APP_ID=`grep "tracking URL" < "${SPARK_LOG}"|head -n 1| cut -d "/" -f5`

rm -f ${SPARK_LOG}

# Send spark run result to elasticsearch for logging
ES_URL="${LOG_ELASTICSEARCH_URL}${LOG_ELASTICSEARCH_INDEX_NAME}"
NOW=`date "+%s"

if [ ${FINAL_STATUS} == "SUCCEEDED" ]; then
  echo "${CLASS_NAME_TO_LOAD} - Process succeeded - "$(date +"%Y-%m-%d %H:%M:%S")
  curl --location --request POST ${ES_URL} \
  --header 'Content-type: application/json' \
  --header 'Cache-Control: no-cache' \
  --user "${LOG_ELASTICSEARCH_USER}:${LOG_ELASTICSEARCH_PWD}" \
  --d "{\"process\":\"${CLASS_NAME_TO_LOAD}\",\"status\":\"SUCCEESS\",\"date\":\"${NOW}\",}"
  exit 0
else
  echo "${CLASS_NAME_TO_LOAD} - Process failed - "$(date +"%Y-%m-%d %H:%M:%S")
  curl --location --request POST ${ES_URL} \
  --header 'Content-type: application/json' \
  --header 'Cache-Control: no-cache' \
  --user "${LOG_ELASTICSEARCH_USER}:${LOG_ELASTICSEARCH_PWD}" \
  --d "{\"process\":\"${CLASS_NAME_TO_LOAD}\",\"status\":\"FAILURE\",\"date\":\"${NOW}\",}"
  exit 1
fi

