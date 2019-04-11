 #!/bin/bash
 curl -XDELETE ${1}/resources-fidmove-test
 curl -XPUT ${1}/resources-fidmove-test -d '{"mappings":{"schemaorg":{"date_detection":false}}}' -H 'Content-Type: Application/json'
 ~/git/efre-lod-elasticsearch-tools/processing/merge2move.py -server ${1}/resources/schemaorg | esbulk -server ${1} -index resources-fidmove-test -type schemaorg -id identifier -w 1 -verbose
 for i in  ~/git/efre-lod-elasticsearch-tools/processing/*.py; do ${i} -server ${1}/resources-fidmove-test/schemaorg | esbulk -server ${1} -index resources-fidmove-test -type schemaorg -w 1 -verbose -id identifier ; done
 curl -XDELETE ${1}/resources-fidmove
 ~/git/efre-lod-elasticsearch-tools/helperscripts/es2json.py -server ${1}/resources-fidmove-test -headless | esbulk -server ${1} -index resources-fidmove -type schemaorg -id identifier -w 1 -verbose
