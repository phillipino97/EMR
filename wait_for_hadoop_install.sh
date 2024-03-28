#!/bin/bash

aws s3 cp s3://map-reduce-dist-system/lambda-emr/bootstrap/bootstrap_setup_nodes.sh .
chmod +x bootstrap_setup_nodes.sh
nohup ./bootstrap_setup_nodes.sh &>/dev/null &