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

java -cp $HOME/dist/interop.jar:$HOME/dist/lib/JSON-java.jar:$HOME/dist/lib/mongo-java-driver-2.12.3.jar:$HOME/dist/lib/mariadb-java-client-1.1.7.jar:$HOME/dist/lib/postgresql-9.3-1102.jdbc41.jar org.bireme.interop.SQL2Mongo "$@"
