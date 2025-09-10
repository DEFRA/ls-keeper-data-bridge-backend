#!/bin/bash
sed -i 's/\r$//' "$0"

export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

set -e

# S3 buckets
echo "Bootstrapping S3 setup..."

## Check if bucket already exists
existing_bucket=$(awslocal s3api list-buckets \
  --query "Buckets[?Name=='test-external-bucket'].Name" \
  --output text)

if [ "$existing_bucket" == "test-external-bucket" ]; then
  echo "S3 bucket already exists: test-external-bucket"
else
  awslocal s3api create-bucket --bucket test-external-bucket --region eu-west-2 \
    --create-bucket-configuration LocationConstraint=eu-west-2 \
    --endpoint-url=http://localhost:4566
  echo "S3 bucket created: test-external-bucket"
fi

echo "Bootstrapping SNS setup..."

# Create SNS Topics
topic_arn=$(awslocal sns create-topic \
  --name ls-keeper-data-bridge-events \
  --endpoint-url=http://localhost:4566 \
  --output text \
  --query 'TopicArn')

echo "SNS Topic created: $topic_arn"
