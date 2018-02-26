CURL=/usr/bin/curl
UNZIP=/usr/bin/unzip
RM=/bin/rm

GIT=/home/bhering/slub-lod-elasticsearch-tools
W2P=https://mdipierro.pythonanywhere.com/examples/static/web2py_src.zip
PATH=/tmp

default: install

web2py.zip:
	${CURL} ${W2P} -o web2py.zip

web2py: web2py.zip
	${UNZIP} web2py.zip -d ${PATH}

install_cntrl: web2py
	${cp} -rv ${GIT}/adlookup/controllers ${PATH}/web2py/applications/welcome

install_view: web2py
	${cp} -rv ${GIT}/adlookup/views ${PATH}/web2py/applications/welcome

install_mod: web2py
	${cp} -rv ${GIT}/adlookup/modules ${PATH}/web2py/applications/welcome

install: install_cntrl install_view install_mod

clean:
	${RM} web2py.zip

uninstall:
	${RM} -rf ${PATH}/web2py
