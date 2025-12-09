#!/bin/bash

SHADOWTRAFFIC_FILE=${1}
CONFLUENT_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='"${CONFLUENT_API_KEY}"' password='"${CONFLUENT_API_SECRET}"';"
SCHEMA_REGISTRY_AUTH="${SCHEMA_REGISTRY_API_KEY}":"${SCHEMA_REGISTRY_API_SECRET}"

if [[ ! -f .env ]]; then
    echo ".env file not found"
    exit 1
fi

if [[ ! -f ${SHADOWTRAFFIC_FILE} ]]; then
    echo "Usage: $0 <shadowtraffic_json_file>"
    exit 1
fi

# Run the Docker container with the dynamically constructed auth string
docker run --rm \
        -e CONFLUENT_BOOTSTRAP_SERVERS \
        -e CONFLUENT_SASL_JAAS_CONFIG="${CONFLUENT_SASL_JAAS_CONFIG}" \
        -e SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
        -e SCHEMA_REGISTRY_AUTH="${SCHEMA_REGISTRY_AUTH}" \
        -e LICENSE_EDITION \
        -e LICENSE_LEASE_KEY \
        -e LICENSE_LEASE_SIGNATURE \
        -e LEASE_TEAM \
        -e LEASE_ORGANIZATION \
        -e LEASE_EXPIRATION \
        -e LEASE_SIGNATURE \
        -v $(pwd)/${SHADOWTRAFFIC_FILE}:/home/config.json \
    shadowtraffic/shadowtraffic:latest --config /home/config.json

