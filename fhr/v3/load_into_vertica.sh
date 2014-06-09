#!/bin/bash

VERTICA_HOST=REPLACE_WITH_VERTICA_HOST
VERTICA_USER=REPLACE_WITH_VERTICA_USERNAME
VERTICA_PASS=REPLACE_WITH_VERTICA_PASSWORD
VSQL_BIN=REPLACE_WITH_PATH_TO_VSQL

# Threshold for diff in no of records imported vs src

THRESHOLD_NDIFF=0.0001 # %

usage() {
    echo "$0 tbl_name file_name" >&2
    exit 2
}

if [ $# -ne 2 ]; then
    usage
fi

SQL_CMD="COPY ${1} FROM LOCAL '${2}' DELIMITER E'\001';"

${VSQL_BIN} -h ${VERTICA_HOST} -U ${VERTICA_USER} -w "${VERTICA_PASS}" -c "${SQL_CMD}"  > "${2}".vertica.log 2>&1

if [ $? -ne 0 ]; then
    echo "Load into vertica failed for ${1} ${2}" >&2
    exit 1
fi

WLINES=$(grep -A2 Loaded "${2}".vertica.log | tail -n 1 | awk '{print $1}')
RLINES=$(wc -l ${2} | cut -f 1 -d\  )

NDIFF=$(($RLINES - $WLINES))

FAILED=$(python -c "print int($NDIFF > ($THRESHOLD_NDIFF * $WLINES))") # 0 => failed

if [ ${FAILED} -eq 1 ]; then
    echo "Expected to load $RLINES but could only load $WLINES (threshold: ${THRESHOLD_NDIFF})"  >&2
    exit 1
fi

