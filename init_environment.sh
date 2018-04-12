export BASEPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${BASEPATH}
for i in `ls -d ${BASEPATH}/*/`; do export PYTHONPATH=${PYTHONPATH}:${i}; done
