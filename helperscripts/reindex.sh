#!/bin/bash
for i in `ls -d */`; do
index=`echo ${i} | cut -d / -f 1`
#for index in works tags; do
host=http://${1}:9200
string=${host}/${index}
curl -XDELETE ${string}
echo ""
curl -XPUT ${string} -d '{"mappings":{"schemaorg":{"date_detection":false}}}' -H "Content-Type: application/json"
echo ""
for ldj in `ls ${index}`; do esbulk -server ${host} -type schemaorg -index ${index} -id identifier -w 8  ${index}/${ldj}; done
done
