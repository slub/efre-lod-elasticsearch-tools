#!/bin/bash
curl -XDELETE '$1:9200/resources'
echo ""
curl -XPUT '$1:9200/resources' -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv CreativeWork-records.ldj | esbulk -host $1 -type schemaorg -index resources -id identifier -w 8
curl -XDELETE '$1:9200/orga'
echo ""
curl -XPUT '$1:9200/orga' -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv Organization-records.ldj | esbulk -host $1 -type schemaorg -index orga -id identifier -w 8 
curl -XDELETE '$1:9200/persons'
echo ""
curl -XPUT '$1:9200/persons' -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv Person-records.ldj | esbulk -host $1 -type schemaorg -index persons -id identifier -w 8 
curl -XDELETE '$1:9200/geo'
echo ""
curl -XPUT '$1:9200/geo' -d '{"mappings":{"schemaorg":{"date_detection":false}}}'
echo ""
pv Place-records.ldj | esbulk -host $1 -type schemaorg -index geo -id identifier -w 8 
