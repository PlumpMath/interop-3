/*=========================================================================

    Copyright © 2014 BIREME/PAHO/WHO

    This file is part of Interop.

    Interop is free software: you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public License as
    published by the Free Software Foundation, either version 2.1 of
    the License, or (at your option) any later version.

    Interop is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with Interop. If not, see <http://www.gnu.org/licenses/>.

=========================================================================*/

package org.bireme.interop;

import org.bireme.interop.fromJson.Json2Couch;
import org.bireme.interop.toJson.Mongo2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class Mongo2Couch extends Source2Destination {

    public Mongo2Couch(final Mongo2Json m2j, 
                       final Json2Couch j2c) {
        super(m2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: Mongo2Couch <mongohost> <mongodb> <mongocol>"
                             + " <mongoquery> <couchhost> <couchdb> OPTIONS");
        System.err.println();
        System.err.println("       <mongohost> - MongoDB server url.");
        System.err.println("       <mongodb> - MongoDB database.");
        System.err.println("       <mongocol> - MongoDB colection name.");
        System.err.println("       <mongoquery> - Search to retrieve the documents"
                                     + "to be exported from MongoDB database.");
        System.err.println("       <couchhost> - CouchDB server url.");
        System.err.println("       <couchdb> - CouchDB database.");        
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --mongoport=<port>");
        System.err.println("           MongoDb server port.");
        System.err.println("       --mongouser=<user>");
        System.err.println("           MongoDB server user.");
        System.err.println("       --mongopsw=<password>");   
        System.err.println("           MongoDB server password.");
        System.err.println("       --mongofrom=<from>");          
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --mongoto=<to>");                
        System.err.println("           Last sequential response hit number.");
        System.err.println("       --couchport=<port>");
        System.err.println("           CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           CouchDB server user.");
        System.err.println("       --couchpsw=<password>");   
        System.err.println("           CouchDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --append");        
        System.err.println("           Appends the documents into the destination CouchDB database.");
        
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception  {        
        final int len = args.length;
        if (len < 6) {
            usage();
        }
        
        final String mongoHost = args[0];
        final String mongoDbName = args[1];
        final String mongoColName = args[2];
        final String mongoQuery = args[3];
        final String couchHost = args[4];
        final String couchDbName = args[5];
        
        String mongoPort = Integer.toString(Mongo2Json.DEFAULT_MONGO_PORT);
        String mongoUser = null;
        String mongoPswd = null;
        int mongoFrom = 1;
        int mongoTo = Integer.MAX_VALUE;
                                      
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 6; idx < len; idx++) {
            if (args[idx].startsWith("--mongoport=")) {
                mongoPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--mongouser=")) {
                mongoUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--mongopsw=")) {
                mongoPswd = args[idx].substring(11); 
            } else if (args[idx].startsWith("--mongofrom=")) {
                mongoFrom = Integer.parseInt(args[idx].substring(12)); 
            } else if (args[idx].startsWith("--mongoto=")) {
                mongoTo = Integer.parseInt(args[idx].substring(10)); 
            } else if (args[idx].startsWith("--couchport=")) {   
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpswd=")) {
                couchPswd = args[idx].substring(12);
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--append")) {
                append = true;
            } else { 
                usage();
            }
        }
        
        final Mongo2Json m2j = new Mongo2Json(mongoHost,
                                              mongoPort,
                                              mongoUser,
                                              mongoPswd,
                                              mongoDbName,
                                              mongoColName);
        m2j.setQuery(mongoQuery, mongoFrom, mongoTo);
        
        final Json2Couch j2c = new Json2Couch(couchHost,
                                              couchPort,
                                              couchUser,
                                              couchPswd,
                                              couchDbName,
                                              append);
        
        new Mongo2Couch(m2j, j2c).export(tell);
    }
}
