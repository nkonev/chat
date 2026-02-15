#!/usr/bin/env bash

PG_USER="$1"
DB_NAME="$2"
MAX_ATTEMPTS="$3"

echo "Waiting for $PG_USER"

set +e

for ((n=0; n<=$MAX_ATTEMPTS; n++))
do
  echo "attempt $n / $MAX_ATTEMPTS"

  output=`docker compose exec postgresql-citus-coordinator-1 psql -U ${PG_USER} -d ${DB_NAME} --csv --tuples-only -c 'SELECT nodename FROM citus_nodes;'`

  has_coordinator_1=false
  has_worker_1=false
  has_worker_2=false

  echo -n $output | grep 'postgresql-citus-coordinator-1' &> /dev/null
  if [[ $? == 0 ]]; then
    has_coordinator_1=true
  fi

  echo -n $output | grep 'postgresql-citus-worker-1' &> /dev/null
  if [[ $? == 0 ]]; then
    has_worker_1=true
  fi

  echo -n $output | grep 'postgresql-citus-worker-2' &> /dev/null
  if [[ $? == 0 ]]; then
    has_worker_2=true
  fi

  if [[ "$has_coordinator_1" = true && "$has_worker_1" = true && "$has_worker_2" = true ]]; then
    echo "PostgreSQL is available"
    exit 0
  fi

  sleep 1
done

echo "Time is out"
exit 1
