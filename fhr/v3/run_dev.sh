#!/bin/bash

# Script that drives the tools to transform and summarize v3 data 
#

if [ "${SCRIPTS_PATH}"x = x ]; then
    SCRIPTS_PATH=${HOME}/prod/ETL-frontoffice-v3/
fi

HADOOP_QUEUE="research"
HDFS_DEST_BASE="/user/${USER}/fhr/frontoffice/3/"
TBL_MAP="${SCRIPTS_PATH}/vertica_table_map.txt"
JOBS="search_counts profile_counts profile_age profile_age_buckets"

export HADOOP_HOME=/opt/cloudera/parcels/CDH/
export HADOOP_MAPRED_HOME=/opt/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/

SNAPSHOT_DATE=$(date +"%Y%m%d")

if [ $# -eq 1 ]; then
    SNAPSHOT_DATE=$(date -d "$1" +"%Y%m%d")
    
    if [ $? -ne 0 ]; then
        usage
    fi
fi

usage() {
    echo "$0 [date(%Y%m%d)]" >&2
    exit 2
}

hadoop dfs -stat ${HDFS_DEST_BASE}/${SNAPSHOT_DATE}/_SUCCESS > /dev/null 2>&1

if [ $? -ne 0 ]; then
    hadoop dfs -rmr ${HDFS_DEST_BASE}/${SNAPSHOT_DATE} >/dev/null 2>&1

    hadoop dfs -mkdir ${HDFS_DEST_BASE}/${SNAPSHOT_DATE} >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Unable to create date prefix dir under ${HDFS_DEST_BASE}" >&2
        exit 1
    fi
fi

TMPDIR=`mktemp -d /tmp/frontoffice_etl_v3.XXXXXX || exit 1`
cd ${TMPDIR}

export PYTHONPATH=${SCRIPTS_PATH}

FAILED_JOBS=""

report_and_die() {
    echo "Job ${job} failed - output in ${TMPDIR}" >&2
    exit 1
}
    
hadoop dfs -stat ${HDFS_DEST_BASE}/${SNAPSHOT_DATE}/_SUCCESS > /dev/null 2>&1

if [ $? -ne 0 ]; then
    for job in $JOBS; do
        python ${SCRIPTS_PATH}/${job}_job.py \
            -r hadoop  \
            --python-archive=${SCRIPTS_PATH}/lib.zip \
            --no-output \
            --jobconf mapred.reduce.tasks=1 \
            --jobconf mapred.job.name="${job} v3 ${SNAPSHOT_DATE}" \
            --jobconf mapred.job.queue.name=${HADOOP_QUEUE} \
            --jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
            --output-dir=${HDFS_DEST_BASE}/${SNAPSHOT_DATE}/${job} \
            --snapshot-date=$(date -d ${SNAPSHOT_DATE} +"%Y-%m-%d")  \
            hdfs:///data/fhr/nopartitions/${SNAPSHOT_DATE}/3/part-r\* > ${job}.log 2>&1

        if [ $? -ne 0 ]; then
            report_and_die
        fi

    done

    hadoop dfs -touchz ${HDFS_DEST_BASE}/${SNAPSHOT_DATE}/_SUCCESS > /dev/null 2>&1
fi

for job in $JOBS; do

    TBL=$(grep ${job}\  ${TBL_MAP} | cut -f 2 -d\  )

    if [ "$TBL"x = x ]; then
        echo "Table for ${TBL} not found in ${TBL_MAP}" >&2
        echo
        report_and_die
    fi
    
    hadoop dfs -getmerge ${HDFS_DEST_BASE}/${SNAPSHOT_DATE}/${job} ${job}.gz >/dev/null 2>&1
    gzip -dc ${job}.gz | sed 's/\t$//' > ${job}

    if [ $? -ne 0 ]; then
        echo "Unable to decompress output of ${job}" >&2
        report_and_die
    fi

    ${SCRIPTS_PATH}/load_into_vertica.sh ${TBL}_${USER} ${job} > vertica_import.log 2>&1
    
    if [ $? -ne 0 ]; then
        echo "Load into vertica failed for ${job}" >&2
        FAILED_JOBS="${FAILED_JOBS} ${job}"
    fi

    rm -f ${job}
done

cd ..

if [ "${FAILED_JOBS}"x != x ]; then
    report_and_die
else
    rm -fr ${TMPDIR}
    exit 0
fi
