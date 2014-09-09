#=========================================================================#
#
#    Copyright © 2014 BIREME/PAHO/WHO
#
#    This file is part of Interop.
#
#    Interop is free software: you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public License as
#    published by the Free Software Foundation, either version 2.1 of
#    the License, or (at your option) any later version.
#
#    Interop is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with Interop. If not, see <http://www.gnu.org/licenses/>.
#
#=========================================================================

#!/bin/bash

HOME=../..

java -cp $HOME/dist/interop.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/lucene-core-4.9.0.jar:$HOME/dist/lib/lucene-analyzers-common-4.9.0.jar:$HOME/dist/lib/lucene-queryparser-4.9.0.jar org.bireme.interop.Twitter2Lucene "$@"
