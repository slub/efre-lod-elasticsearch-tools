# EFRE Linked Open Data ElasticSearch Toolchain
Collection of python3 tools/programs/daemons to map bibliographic metadata to a usable LOD-schema into JSON-LD. The base of the source-data must be MARC21 (bibliographic and authority) and it can be enriched using various webservices. Prerequisits is to have this data in a Elasticsearch-Index in marcXchange format. Use this tool to transpose binary or XML formatted MARC21: [marc2jsonl](https://github.com/slub/marc2jsonl). To ingest your Line-delimited JSON rawdata and the transformed to Elasticsearch, the best tool is [esbulk](https://github.com/miku/esbulk), but use the -id switch for your Pica Product Numbers (001). In our case, we use [Schema.org](https://schema.org), but you can use your own schema, just adjust esmarc.py.

##### Table of Contents

[initialize environment](#init_environment.sh)

[getindex](#es2json.py)

[esmarc](#esmarcpy)

[entityfacts-bot](#entityfacts-bot.py)

[ldj2rdf](#ldj2rdf.py)

[lido2schema](#lido2schema.py)

[gnd2swb](#gnd2swb.py)


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

<a name="esmarc.py"/>

# esmarc.py

esmarc is a python3 tool to read line-delimited MARC21 JSON from an elasticSearch index, perform a mapping and writes the output in a directory with a file for each mapping type.

dependencies:
python3-elasticsearch

run:

```
$ esmarc.py <OPTARG>
	-h, --help            show this help message and exit
	-host HOST            hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.
	-port PORT            Port of the ElasticSearch-node to use, default is 9200.
	-type TYPE            ElasticSearch Type to use
	-index INDEX          ElasticSearch Index to use
	-id ID                map single document, given by id
	-help                 print this help
	-prefix PREFIX        Prefix to use for output data
	-debug                Dump processed Records to stdout (mostly used for debug-purposes)
	-server SERVER        use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty.
	-pretty               output tabbed json
	-w W                  how many processes to use
	-idfile IDFILE        path to a file with IDs to process
	-query QUERY          prefilter the data based on an elasticsearch-query
	-generate_ids         switch on if you wan't to generate IDs instead of looking them up. usefull for first-time ingest or debug purposes
	-lookup_host LOOKUP_HOST Target or Lookup Elasticsearch-host, where the result data is going to be ingested to. Only used to lookup IDs (PPN) e.g. http://192.168.0.4:9200

```

examples:



`$ esmarc.py -server http://127.0.0.1/source/mrc`

transforms the marcXchange data to line-delimited schema.org data.

creates a output directory for every type of entity set up.

<a name="entityfacts-bot.py"/>

# entityfacts-bot.py 

entityfacts-bot.py is a Python3 program that enrichs ("links") your data with more identifiers from entitiyfacts.  Prerequisits is that you have a field containing your GND-Identifier.


It connects to an elasticsearch node and outputs the enriched data, which can be put back to the index using esbulk.

## Usage

```
./entityfacts-bot.py
	-h, --help            show this help message and exit
	-host HOST            hostname or IP-Address of the ElasticSearch-node to use, default is localhost.
	-port PORT            Port of the ElasticSearch-node to use, default is 9200.
	-index INDEX          ElasticSearch Search Index to use
	-type TYPE            ElasticSearch Search Index Type to use
	-id ID                retrieve single document (optional)
	-searchserver SEARCHSERVER use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
	-stdin                get data from stdin
	-pipeline             output every record (even if not enriched) to put this script into a pipeline

```


## Requirements

python3-elasticsearch

e.g. (ubuntu)
```
sudo apt-get install python3-elasticsearch
```
<a name="entityfacts-bot.py"/>

# wikidata.py 

wikidata.py is a Python3 program that enrichs ("links") your data with the wikidata-identifier from wikidata.  Prerequisits is that you have a field containing your GND-Identifier. Other identifiers are planned to be used in future.


It connects to an elasticsearch node and outputs the enriched data, which can be put back to the index using esbulk.

## Usage

```
./wikidata.py
	-h, --help      show this help message and exit
	-host HOST      hostname or IP-Address of the ElasticSearch-node to use, default is localhost.
	-port PORT      Port of the ElasticSearch-node to use, default is 9200.
	-index INDEX    ElasticSearch Search Index to use
	-type TYPE      ElasticSearch Search Index Type to use
	-id ID          retrieve single document (optional)
	-stdin          get data from stdin
	-pipeline       output every record (even if not enriched) to put this script into a pipeline
	-server SERVER  use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty
```


## Requirements

python3-elasticsearch
python3-rdflib

e.g. (ubuntu)
```
sudo apt-get install python3-elasticsearch python3-rdflib
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


<a name="lido2schema.py"/>

##TODO
ElasticSearch ingest
<a name="gnd2swb.py"/>

# sameAs2id.py

This small pythonscripts converts the D-NB or SWB IDs in your elasticSearch bibliographic works or authority index which are referring to other bib/auth data to @id. You need several indices for all authority entitys. The output is the enriched data which can be put back into the index by piping it into esbulk.

## Usage

```
./sameAs2id.py
	-h, --help            show this help message and exit
	-host HOST            hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.
	-port PORT            Port of the ElasticSearch-node to use, default is 9200.
	-type TYPE            ElasticSearch Index to use
	-index INDEX          ElasticSearch Type to use
	-help                 print this help
	-stdin                get data from stdin
	-id ID                enrich a single id
	-server SERVER        use http://host:port/index/type/id syntax. overwrites host/port/index/id/pretty
	-searchserver SEARCHSERVER search instance to use. default is -server e.g. http://127.0.0.1:9200
	-pipeline             output every record (even if not enriched) to put this script into a pipeline

```

## Requirements

python3-elasticsearch





