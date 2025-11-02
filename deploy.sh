#!/usr/bin/env bash


result=$(aws sts get-caller-identity --output text --query 'Account') && export AWS_ACCOUNT=$result || { echo "could not get account ID"; exit 1; }

echo "===> Building src"
mkdir -p build
cp -r src/* build/
pip install -t build/ -r requirements.txt --upgrade
pip install flake8 bandit black

echo "===> Testing src"
flake8 src
bandit -r src

SERVICE_NAME=${PWD##*/}
REGIONS=(
  "us-east-1"
#  "us-west-2"
)

echo $AWS_ACCOUNT
echo $SERVICE_NAME
echo $REGIONS

for REGION in ${REGIONS[@]}; do
  echo "===> Packaging and deploy for $REGION"
  echo coxauto-rpp-$REGION-$AWS_ACCOUNT-temp
  OUTPUT_FILE=packaged-$SERVICE_NAME-$REGION.yml

  sam package \
    --template-file template.yml \
    --region $REGION \
    --s3-bucket coxauto-rpp-$REGION-$AWS_ACCOUNT-temp \
    --output-template-file $OUTPUT_FILE

  if [ -z "$1" ]
  then
    sam deploy \
      --no-fail-on-empty-changeset \
      --template-file $OUTPUT_FILE \
      --stack-name $SERVICE_NAME \
      --s3-bucket coxauto-rpp-$REGION-$AWS_ACCOUNT-temp \
      --region $REGION \
      --capabilities CAPABILITY_NAMED_IAM
  else
    sam deploy \
      --no-fail-on-empty-changeset \
      --template-file $OUTPUT_FILE \
      --stack-name $1-$SERVICE_NAME \
      --s3-bucket coxauto-rpp-$REGION-$AWS_ACCOUNT-temp \
      --region $REGION \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameter-overrides AliasName="${1}"
  fi
done
