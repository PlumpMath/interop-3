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

import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.Mongo2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140815
 */
public class Mongo2Lucene extends Source2Destination {

    public Mongo2Lucene(final Mongo2Json m2j, 
                        final Json2Lucene j2l) {
        super(m2j, j2l);
    }
    
    private static void usage() {
        System.err.println("usage: Mongo2Lucene <mongohost> <mongodb> <mongocol>"
                               + " <mongoquery> <lucenedir> OPTIONS");
        System.err.println();
        System.err.println("       <mongohost> - MongoDB server url.");
        System.err.println("       <mongodb> - MongoDB database.");
        System.err.println("       <mongocol> - MongoDB colection name.");
        System.err.println("       <mongoquery> - Search to retrieve the documents"
                                     + "to be exported from MongoDB database.");
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
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
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");        
        System.err.println("           Appends the documents into the destination Lucene index.");
        
        System.exit(1);
    }

    public static void main(final String[] args) throws Exception {        
        final int len = args.length;
        if (len < 5) {
            usage();
        }
        
        final String mongoHost = args[0];
        final String mongoDbName = args[1];
        final String mongoColName = args[2];   
        final String mongoQuery = args[3];
        final String luceneDir = args[4];
        
        String mongoPort = Integer.toString(Mongo2Json.DEFAULT_MONGO_PORT);
        String mongoUser = null;
        String mongoPswd = null;                                      
        int mongoFrom = 1;
        int mongoTo = Integer.MAX_VALUE;
        
        boolean store = false;
        boolean append = false;        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 5; idx < len; idx++) {
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
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--store")) {
                store = true;
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
        
        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new Mongo2Lucene(m2j, j2l).export(tell);
    }
}
