#!/bin/bash
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

## Optional: Add a test object
echo "Adding test object to S3 bucket..."
echo "Alive" > /tmp/test-object.txt
awslocal s3 cp /tmp/test-object.txt s3://test-external-bucket/test-object.txt

## Optional: List contents
echo "Listing contents of test-external-bucket:"
awslocal s3 ls s3://test-external-bucket/
