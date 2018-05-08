SOURCES=$(shell find . -name "*.mrc")
OBJECTS=$(SOURCES:%.mrc=%.ldj)
LOD_GIT=~/git/efre-lod-elasticsearch-tools/
OUTPUT_DIR=.

.PHONE: all clean

all: main

ldj: $(OBJECTS)

schemaorg: $

%.ldj: %.mrc	
	marc2jsonl -i $< | $(LOD_GIT)/helperscripts/fix_mrc_id.py > $(OUTPUT_DIR)/$@

