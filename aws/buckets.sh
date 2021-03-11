#!/bin/bash
set -x
aws --profile localstack configure set aws_access_key_id "test-key"
aws --profile localstack configure set aws_secret_access_key "test-secret"
aws --profile localstack configure set aws_region "us-east-1"
aws s3 mb s3://quarkus.s3.quickstart --profile localstack --endpoint-url=http://0.0.0.0:4566
set +x