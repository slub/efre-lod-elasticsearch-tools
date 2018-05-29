#!/bin/bash
for i in `ls -d */`; do
index=`echo ${i} | cut -d / -f 1`
string=${1}:9200/${index}
curl -XDELETE ${string}
echo ""
curl -XPUT ${string} -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv ${index}/*-records.ldj | esbulk -host $1 -type schemaorg -index ${index} -id identifier -w 8
done
