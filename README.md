# getindex.py
simple download-script for saving a ElasticSearch-index into a line-delimited JSON-File

it reads some cmdline-arguments and prints the data to stdout. you can pipe it into your next processing-programm or save it via > to a file.

## Usage

```
getindex.py
        -help      print this help
	-host      hostname or IP-Address of the ElasticSearch-Node to use, default is localhost
	-port	   port of the ElasticSearch-node to use, default is 9200
	-index	   ElasticSearch Index to use
	-type	   ElasticSearch doc_type
	-body	   Select specific fields or body
```

## Requirements
python-elasticsearch

e.g.
```
sudo apt-get install python3-elasticsearch
```

# esmarc
esmarc is a python3 tool to read line-delimited JSON from a file, from stdin or from an elasticSearch index, perform a simple mapping and writes out to stdout, a file or writes it via the bulk-endpoint into a new elasticSearch index.

dependencies:
python3-elasticsearch

run:

`$ esmarc.py <OPTARG>`

valid options are:

-host 		hostname or IP-Address of the ElasticSearch-node to use for Input. If None we read from a file or stdin.

-port   	Port of the ElasticSearch-node which is set by -host. default is 9200.

-index		ElasticSearch index to use to harvest the data.

-type		ElasticSearch type to use to harvest the data.

-tohost 	hostname or IP-Address of the ElasticSearch-node to use for Output.

-toport		Port of the ElasticSearch-node which is set by -tohost. default is 9200.

-same		Select this switch if the source- and target ElasticSearch-node are the same.

-toindex	ElasticSearch Index to use to ingest the processed data.

-show\_schaemas	show the schemas which are defined in the sourcecode.

-schema		select the schema which should be defined in the sourcecode. also used for the doc\_type if ingested in a new elasitcsearch-index

-i		Input file path. Default is stdin if no arg is given.

-o		Output file path. Default is stdout if no arg is given.

examples:

`$ esmarc.py -i input-ldj -o output.jsonl -schema schemaorg`

transforms the marc-ldj data to line-delimited schema.org data.

`$ esmarc.py -host 127.0.0.1 -index source -type mrc -schema bibframe > output.ldj`

harvests the data from localhost and prints the data to output.ldj in bibfra.me format.

`$ esmarc.py -host 127.0.0.1 -index source -type mrc -same -toindex newindex -schema bibframe`

harvests the data from localhost and puts the transformed data to 127.0.0.1:/newindex/bibframe

# esfstats-python - elasticsearch fields statistics

esfstats-python is a Python program that extracts some statistics re. field coverage in an ElasticSearch Index.

you need to install elasticsearch-python

## Usage

```
esfstats 
        -host   hostname or IP of the elasticsearch instance
        -port   port of the native Elasticsearch transport protocol API
        -index  index name
        -type   document type
        -help   print this help
        -marc   ignore Marc identifier field if you are analysing an index of marc records
```

# entityfacts-bot.py - enrich your elasticSearch index with facts from entityfacts

entityfacts-bot.py is a Python3 program that enrichs your elasticSearch index with facts and data from entitiyfacts.  Prerequisits is that you have a field containing your GND-Identifier. Default is a schema.org mapping but you can adjust the mapping via the schema2entity python-dict(). On the right side of that dict() you have to fill in your keys, on the left side are the keys of entityfacts. visit http://hub.culturegraph.org/entityfacts/context/v1/entityfacts.jsonld for a list of supported keys.


It connects to an elasticSearch node and updates the given index.

## Usage

```
./entityfacts-bot.py
        -help   print this help
	-host	hostname or IP-Address of the ElasticSearch-node to use
	-port	port of the ElasticSearch-node to use, default is 9200
	-index  ElasticSearch index to use
	-type	ElasticSearch doc_type to use
```

## Requirements

python3-elasticsearch

e.g. (ubuntu)
```
sudo apt-get install python3-elasticsearch
```


# lido2schema.py - transform lido metadata to schema.org

This small pythonscript transforms the lido metadata to schema.org. The outcome is line-delimited json as well and can be indexed to an ElasticSearch-Index via esbulk.

prerequisites: transform the XML data via helperscripts/xml-json (uses node.js) to line-delimited json.

## Requirements

python-dpath

```
pip install dpath
```

##TODO
ElasticSearch ingest


# fieldstats-ldj.py - return occurency statistics of a single field of an line delimited json stream

This small pythonscript reads a line delimited json stream and returns the occurency statistics of a field given via -path Parameter.

## Requirements

python3-numpy





