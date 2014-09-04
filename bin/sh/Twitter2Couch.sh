#!/bin/bash

HOME=../..

java -cp $HOME/dist/interop.jar:$HOME/dist/lib/JSON-java.jar org.bireme.interop.Twitter2Couch "$@"
