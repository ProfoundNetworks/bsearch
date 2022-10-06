#!/bin/bash
# Run some benchmarks using the domain grade injector dataset on S3
#
# Once the data is downloaded, you should be seeing approximately the following
# difference in performance between the original and improved indexers.
# 
# Original:
#
# $ git rev-parse --short HEAD
# b39342b
# $ ./benchmark_dgin.sh
# + mkdir -p gitignore
# + '[' '!' -f gitignore/data ']'
# + bash -c 'cd cmd/bsearch; go build'
# + bash -c 'cd cmd/bsearch_index; go build'
# + cmd/bsearch_index/bsearch_index --sep '|' --force gitignore/data
# 
# real    1m40.749s
# user    2m2.485s
# sys     0m25.125s
# + cmd/bsearch/bsearch google.com gitignore/data
# google.com|A
# 
# real    0m18.772s
# user    0m22.671s
# sys     0m2.186s
#
# Improved:
#
# $ git rev-parse --short HEAD
# 5fbf101
# $ ./benchmark_dgin.sh
# + mkdir -p gitignore
# + '[' '!' -f gitignore/data ']'
# + bash -c 'cd cmd/bsearch; go build'
# + bash -c 'cd cmd/bsearch_index; go build'
# + cmd/bsearch_index/bsearch_index --sep '|' --force gitignore/data
# 
# real    0m56.637s
# user    1m8.842s
# sys     0m4.370s
# + cmd/bsearch/bsearch google.com gitignore/data
# google.com|A
# 
# real    0m1.608s
# user    0m1.659s
# sys     0m0.643s
#
set -euxo pipefail

mkdir -p gitignore
if [ ! -f "gitignore/data" ]; then
    aws s3 cp s3://databrewery/quarterly_injector/2022q2/data.gz gitignore
    gunzip gitignore/data.gz
fi

bash -c "cd cmd/bsearch; go build"
bash -c "cd cmd/bsearch_index; go build"

time cmd/bsearch_index/bsearch_index --sep '|' --force gitignore/data
time cmd/bsearch/bsearch google.com gitignore/data
