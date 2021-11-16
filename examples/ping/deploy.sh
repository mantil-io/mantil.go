#!/usr/bin/env bash -e

echo "build and prepare zip package"
GOOS=linux GOARCH=amd64 go build -o bootstrap
zip fn.zip bootstrap

function_name=mantil-go-ping-example

if $(aws lambda get-function --function-name $function_name > /dev/null 2>&1); then
    echo "update existing function"
    aws lambda update-function-code --function-name $function_name --zip-file fileb://fn.zip --no-cli-pager
else
    echo "create new function"
    role_name="$function_name-role"
    aws iam create-role --role-name $role_name --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}' --no-cli-pager
    role_arn=$(aws iam get-role --role-name $role_name | jq .Role.Arn -r)
    aws iam attach-role-policy --role-name $role_name --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole --no-cli-pager
    sleep 10 # lame way to get policy ready
    aws lambda create-function --function-name $function_name --runtime provided.al2 --zip-file fileb://fn.zip --role=$role_arn --handler=provided --no-cli-pager
fi

rm fn.zip bootstrap
