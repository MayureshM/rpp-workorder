#!/bin/bash

# Script to delete AWS CloudWatch log groups containing a specified substring
# Usage: ./remove_log_groups.sh <substring>
# Example: ./remove_log_groups.sh /aws/lambda/my-lambda
# Note: This script requires the AWS CLI and jq to be installed
#      You must have the necessary permissions to delete log groups
#      Use at your own risk

# Its likely that you will need to run this script between the deletion of your alias stacks
# the cloudformation template re-uses the log group names and if you delete the stack, the log group will not be deleted

# Function to display usage error
usage() {
    echo "Usage: $0 <substring>"
    exit 1
}

# Check if substring is passed in
if [ -z "$1" ]; then
    usage
fi

SUBSTRING=$1

# Fetch and filter log groups
log_groups=$(aws logs describe-log-groups --output json | jq -r --arg substr "$SUBSTRING" '.logGroups[] | select(.logGroupName | contains($substr)) | .logGroupName')

# Check if any log groups match the substring
if [ -z "$log_groups" ]; then
    echo "No log groups found containing the substring '$SUBSTRING'."
    exit 0
fi

# Display log groups expected to be deleted
echo "The following log groups will be deleted:"
echo "$log_groups"

# Prompt user for confirmation
read -p "Are you sure you want to delete these log groups? (yes/no): " confirmation

if [ "$confirmation" != "yes" ]; then
    echo "Deletion aborted."
    exit 0
fi

# Delete the log groups
echo "$log_groups" | while read log_group_name; do
    echo "Deleting log group: $log_group_name"
    aws logs delete-log-group --log-group-name "$log_group_name"
done

echo "Deletion completed."
