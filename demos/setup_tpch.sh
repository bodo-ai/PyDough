set -eo pipefail

if [ -e $1 ]; then
    echo "FOUND"
    exit 0
else
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db -O $1
fi
