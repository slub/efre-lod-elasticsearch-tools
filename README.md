# EFRE Linked Open Data ElasticSearch Toolchain
Collection of python3 tools/programs/daemons to harvest RDF out of bibliographic Metadata such as MARC21, FINC-SOLR or LIDO. Prerequisits is to have this data in a Elasticsearch-Index. For MARC21, use this tool: [marc2jsonl](https://github.com/slub/marc2jsonl). LIDO can be transposed from XML to JSON-LD via a [helperscript](../master/helperscripts/xml-json). To ingest your Line-delimited JSON data to Elasticsearch, the best tool is [esbulk](https://github.com/miku/esbulk), but use the -id switch for your Pica Product Numbers. In our case, we use [Schema.org](https://schema.org), but you can use your own schema, just adjust esmarc.py.

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
getindex.py
        -help      print this help
	-host      hostname or IP-Address of the ElasticSearch-Node to use, default is localhost
	-port	   port of the ElasticSearch-node to use, default is 9200
	-index	   ElasticSearch Index to use
	-type	   ElasticSearch doc_type
	-body	   Select specific fields or body
        -server    use http://host:port/index/doc_type/doc_id syntax

```

## Requirements
python-elasticsearch

e.g.
```
sudo apt-get install python3-elasticsearch
```

<a name="esmarc.py"/>

# esmarc

esmarc is a python3 tool to read line-delimited JSON from an elasticSearch index, perform a simple mapping and writes the output in a directory with a file for each mapping type.

dependencies:
python3-elasticsearch

run:

```
$ esmarc.py <OPTARG>
	-host 		hostname or IP-Address of the ElasticSearch-node to use for Input. If None we read from a file or stdin.
	-port   	Port of the ElasticSearch-node which is set by -host. default is 9200.
	-index		ElasticSearch index to use to harvest the data.
	-type		ElasticSearch type to use to harvest the data.
        -id		map a single document, given by id.
        -pretty		output tabbed json.
	-schema		select the schema which should be defined in the sourcecode. also used for the doc_type if ingested in a new elasitcsearch-index
        -server         use http://host:port/index/doc_type/doc_id syntax. overwrites host, port, index, type, id.
```

examples:



`$ esmarc.py -host 127.0.0.1 -index source -type mrc`

transforms the marcXchange data to line-delimited schema.org data.

creates a output directory for every type of entity set up.



<a name="entityfacts-bot.py"/>

# entityfacts-bot.py 

entityfacts-bot.py is a Python3 program/daemon that enrichs your elasticSearch index with facts and data from entitiyfacts.  Prerequisits is that you have a field containing your GND-Identifier. Default is a schema.org mapping but you can adjust the mapping via the schema2entity python-dict(). On the right side of that dict() you have to fill in your keys, on the left side are the keys of entityfacts. visit http://hub.culturegraph.org/entityfacts/context/v1/entityfacts.jsonld for a list of supported keys. It can be either run standalone or as a service. In case of running it as a service there are two options. Either it runs in the background and enriches all the data in the specified index or it opens a TCP Socket to wait for a list of id's to enrich in the elasticsearch Index. Configuration can also be done over a json-formatted file.


It connects to an elasticsearch node and updates the given index.

## Usage

```
./entityfacts-bot.py
        -help    	print this help
	-host	 	hostname or IP-Address of the ElasticSearch-node to use
	-port	 	port of the ElasticSearch-node to use, default is 9200
	-index   	ElasticSearch index to use
	-type	 	ElasticSearch doc_type to use
	-debug	 	don't daemonize
	-file	 	file with line-delimited id's to enrich
	-start	 	start the daemon
	-stop	 	stop the daemon
	-restart 	restart the daemon
	-full_index	enrich the full index
	-pid_file	Path to store the pid_file of the daemon
	-listen		listen for IDs on a open TCP-socket connection
	-conf		Path to load the configuration
```
## configuration example
```
/etc/conf.d/entityfacts-bot.cfg
{
"host"		: "127.0.0.1",
"port"		: "9200",			
"ef_host"	: "127.0.0.1",
"ef_port"	: "6969",
"index"		: "source-schemaorg",
"type"		: "schemaorg,"
"pid_file"	" "/var/tmp/entityfacts.pid"
}
```


## Requirements

python3-elasticsearch

e.g. (ubuntu)
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
```

## Requirements

python3-rdflib
python3-elasticsearch


<a name="lido2schema.py"/>

# lido2schema.py 

This small pythonscript transforms the lido metadata to schema.org. The outcome is line-delimited json as well and can be indexed to an ElasticSearch-Index via esbulk.

prerequisites: transform the XML data via helperscripts/xml-json (uses node.js) to line-delimited json.

## Requirements

python-dpath

```
pip install dpath
```

##TODO
ElasticSearch ingest
<a name="gnd2swb.py"/>

# gnd2swb.py

This small pythonscripts converts the D-NB IDs in your elasticSearch bibliographic works index to SWB IDs. You need an Index with both SWB and D-NB IDs.

## Usage

```
./gnd2swb.py
	-host		hostname or IP-Address of the ElasticSearch-index to use
	-port		port of the ElasticSearch-node to use, default is 9200
	-index		ElasticSearch-Index to use
	-type		ElasticSearch-Type to use
	-aut_index	ElasticSearch-Index where to lookup the SWB-IDs (Authority-Index)
	-aut_type	ElasticSearch-Type where to lookup the SWB-IDs (Authority-Index)
	-mp		Enable multiprocessing
```

## Requirements

python3-elasticsearch





