#!/bin/bash
for i in resources orga persons geo; do
string=${1}:9200/${i}
curl -XDELETE ${string}
echo ""
curl -XPUT ${string} -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv ${i}-records.ldj | esbulk -host $1 -type schemaorg -index ${i} -id identifier -w 8
done
