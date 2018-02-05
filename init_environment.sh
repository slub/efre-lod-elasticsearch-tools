for i in `ls -d */`; do export PYTHONPATH=${PYTHONPATH}:`pwd`/${i}; done
