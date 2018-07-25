#!/usr/bin/env bash

# Dates are in ISO8601
LOG_GROUP=$1
START_DATE=$2
END_DATE=$3

awslogs get "$LOG_GROUP" \
  -s "$START_DATE" \
  -e "$END_DATE" \
  --filter-pattern '[report, request_id_label, request_id, duration=Duration*, ...]' \
  | cut -d' ' -f6