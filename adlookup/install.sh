#!/bin/bash
wget https://mdipierro.pythonanywhere.com/examples/static/web2py_src.zip
unzip web2py_src.zip
cp -rv controllers views modules web2py/applications/welcome
cd web2py
echo -e "\n\n"
echo "Run web2py -i [INET-ADRESS_TO_LISTEN]"
