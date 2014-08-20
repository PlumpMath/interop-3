#!/bin/bash

HOME=../..

java -cp $HOME/dist/Interop2.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/mongo-java-driver-2.12.3.jar org.bireme.interop.Mongo2Mongo "$@"
