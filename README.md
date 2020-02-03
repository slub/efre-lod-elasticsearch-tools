# EFRE Linked Open Data ElasticSearch Toolchain
Collection of python3 tools/programs/daemons around [esmarc](https://github.com/slub/esmarc). Prerequisits is to have this data in a Elasticsearch-Index in marcXchange format. Use this tool to transpose binary or XML formatted MARC21: [marc2jsonl](https://github.com/slub/marc2jsonl). To ingest your Line-delimited JSON rawdata and the transformed to Elasticsearch, the best tool is [esbulk](https://github.com/miku/esbulk), but use the -id switch for your Pica Product Numbers (001). In our case, we use [Schema.org](https://schema.org), but you can use your own schema, just adjust esmarc.py.

##### Table of Contents

[initialize environment](#init_environment.sh)

[getindex](#es2json.py)

[ldj2rdf](#ldj2rdf.py)

[lido2schema](#lido2schema.py)


<a name="init_environment.sh" />

# init\_environment.sh

set the PYTHONPATH variable to the path of this git repository and it's subfolders. It also works when called outside of the git repository.

## Usage

. ./init\_environment.sh

<a name="es2json.py"/>

# es2json.py
simple download-script for saving a whole ElasticSearch-index, just the corresponding type or just a document into a line-delimited JSON-File

it reads some cmdline-arguments and prints the data to stdout. you can pipe it into your next processing-programm or save it via > to a file.

also contains some useful functions used in other tools.

## Usage

```
es2json.py  
	-h, --help        show this help message and exit
	-host HOST        hostname or IP-Address of the ElasticSearch-node to use, default is localhost.
	-port PORT        Port of the ElasticSearch-node to use, default is 9200.
	-index INDEX      ElasticSearch Search Index to use
	-type TYPE        ElasticSearch Search Index Type to use
	-source SOURCE    just return this field(s)
	-include INCLUDE  include following _source field(s)
	-exclude EXCLUDE  exclude following _source field(s)
	-id ID            retrieve single document (optional)
	-headless         don't include Elasticsearch Metafields
	-body BODY        Searchbody
	-server SERVER    use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
	-idfile IDFILE    path to a file with newline-delimited IDs to process
	-pretty           prettyprint
```

## Requirements
python-elasticsearch

e.g.
```
sudo apt-get install python3-elasticsearch
```


<a name="ldj2rdf.py"/>

# ldj2rdf.py 

This python3 program/daemon transforms line-delimited json either read in from a file or from an elasticsearch-Index to RDF.

## Usage
```
./ldj2rdf.py
	-help		print this help
	-debug		more debugging output
	-host		hostname or IP-Address of the ElasticSearch-node to use
	-port		port of the ElasticSearch-node to use, default is 9200
	-index		index of the ElasticSearch to use
	-type		doc_type of the ElasticSearch-Index to use
	-scroll		serialize the whole Index to RDF
	-doc		serialize a single document, required parameter is the _id of the document
	-inp		don't use elasticsearch, serialize the RDF out of this line-delimited JSON file
	-context	deliver a url to the context if there is no @context field in the data
	-server		use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id arguments
```

## Requirements

python3-rdflib
python3-elasticsearch


