#!/usr/bin/env bash
set -eo pipefail

bold=$(tput bold 2>/dev/null || true)
normal=$(tput sgr0 2>/dev/null || true)

echo ".____                  .__                  .__        ";
echo "|    |    __ __  _____ |__| ____   ____     |__| ____  ";
echo "|    |   |  |  \/     \|  |/ ___\ /  _ \    |  |/  _ \ ";
echo "|    |___|  |  /  Y Y  \  / /_/  >  <_> )   |  (  <_> )";
echo "|_______ \____/|__|_|  /__\___  / \____/ /\ |__|\____/ ";
echo "        \/           \/  /_____/         \/            ";
echo
echo "${bold}Tracer ingestion deployment${normal}"
function usage() {
    cat <<EOM
    Usage:
        If no parameters are used then local deployment is being chosen.
        $(basename $0) [options]
        [--encrypted-file - Optional. Encrypted file under encrypted_files directory. If not given then using default credential file.
        [--env] - Optional. Environment to use. Default is USER environment.
        [--region] - Optional. Deploy on this aws region. Default is us-west-2 or if .env is defined use the value defined there.
        [--termination-protection] - Optional. If set then the stack will be protected by termination protection.
EOM
    exit 0
}

function subscribe_to_log_shipping() {
    # Allow \n when getting output from running commands
    old_ifs=${IFS}
    IFS=

    timestamp=`date +%s`
    # Getting the list of functions which were just deployed
    functions_list=`sls deploy list functions --env ${env}| grep "Serverless: ${env}" | sed -e 's/.*Serverless: \(.*\): .*/\1/'`
    account_id=`aws sts get-caller-identity --region ${region} --output text --query 'Account'`
    shipper_function_name=${env}_log-shipping_logzio-cloudwatch-log-shipper
    shipper_function_arn="arn:aws:lambda:${region}:${account_id}:function:${shipper_function_name}"

    IFS=${old_ifs}

    for function_name in ${functions_list};
    do
        # In order to create a subscription, each function's log group needs permission to invoke the shipper function
        # Each "statement-id" parameter has to be unique, therefore the "timestamp" addition
        aws lambda add-permission \
        --region ${region} \
        --function-name "${shipper_function_arn}" \
        --statement-id "${function_name}_${timestamp}" \
        --principal "logs.${region}.amazonaws.com" \
        --action "lambda:InvokeFunction" \
        --source-arn "arn:aws:logs:${region}:${account_id}:log-group:/aws/lambda/${function_name}:*" \
        --source-account "${account_id}" > /dev/null
        echo "Successfully added permission for ${function_name} to invoke ${shipper_function_name}"

        # Creating a subscription for the log group to invoke the log shipper function
        aws logs put-subscription-filter \
        --region ${region} \
        --log-group-name "/aws/lambda/${function_name}" \
        --filter-name "cloudwatch" \
        --filter-pattern "" \
        --destination-arn "${shipper_function_arn}"
        echo "Successfully subscribed ${function_name} to ${shipper_function_name}"
    done
}

function deploy() {
    for directory in ./create_aws_resources/* ; do
        if [[ -d "$directory" ]]; then
            echo "${bold}Deploying ${directory}${normal}"
            pushd $directory > /dev/null
            npm i > /dev/null 2>&1
            sls deploy --force --env $env --region $region
            if [[ ${env} != int* ]] ; then
                subscribe_to_log_shipping
            fi
            popd > /dev/null 2>&1
        fi
    done

    if [[ ! -z "$termination_protection" ]]
    then
        echo "Updating termination protection"
        for directory in ./create_aws_resources/* ; do
            if [[ -d "$directory" ]]; then
                pushd $directory > /dev/null
                stack_name=$(sls info --env $env --region $region|grep stack:|awk '{print $2}')
                aws cloudformation update-termination-protection --region $region --enable-termination-protection --stack-name ${stack_name}
                popd > /dev/null 2>&1
            fi
        done
    fi
}

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --help)
            usage
            ;;
        --env)
            opt_env="$2"
            shift # past argument
            shift # past value
            ;;
        --region)
            opt_region="$2"
            shift # past argument
            shift # past value
            ;;
        --encrypted-file)
            encrypted_file="$2"
            shift # past argument
            shift # past value
            ;;
        --termination-protection)
            termination_protection="True"
            shift # past argument
            ;;
        *)
            echo "Unknown argument ${1}. Aborting."
            exit 1
    esac
done

env=${opt_env:-${USER}}
region=${opt_region:-us-west-2}

echo "Env: ${env}"
echo "Region: ${region}"

# If no arguments were given then assume local deployment.
if [[ -z "$encrypted_file" ]]
then
    echo "Using local AWS credentials file"
    deploy
else
    enc_location=../common-resources/encrypted_files/$encrypted_file
    if [[ ! -f ${enc_location} ]]
    then
        echo "$enc_location not found"
        exit 1
    fi
    echo "Creating new credential files"
    mkdir -p ~/.aws
    echo ${KEY} | gpg --batch -d --passphrase-fd 0 ${enc_location} > ~/.aws/credentials
    deploy
fi

echo "Done"
