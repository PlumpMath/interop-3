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
import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.Couch2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class Couch2Lucene extends Source2Destination {

    public Couch2Lucene(final Couch2Json c2j, 
                        final Json2Lucene j2l) {
        super(c2j, j2l);
    }
    
    private static void usage() {
        System.err.println("usage: Couch2Lucene <couchhost> <couchdb> "
                                          + "<couchquery> <lucenedir> OPTIONS");
        System.err.println();
        System.err.println("       <couchhost> - CouchDB server url.");
        System.err.println("       <couchdb> - CouchDB database.");
        System.err.println("       <couchquery> - Search to retrieve the documents"
                                     + "to be exported from CouchDB database.");
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --couchport=<port>");
        System.err.println("           CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           CouchDB server user.");
        System.err.println("       --couchpsw=<password>");
        System.err.println("           CouchDB server password.");
        System.err.println("       --couchfrom=<num>");
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --couchto=<num>");
        System.err.println("           Last sequential response hit number.");   
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination Lucene index.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception  {        
        final int len = args.length;
        if (len < 4) {
            usage();
        }
        
        final String couchHost = args[0];
        final String couchDbName = args[1];
        final String couchQuery = args[2];        
        final String luceneDir = args[3];
                
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
        int couchFrom = 1;
        int couchTo = Integer.MAX_VALUE;                
                                             
        boolean store = false;
        boolean append = false;        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 4; idx < len; idx++) {
            if (args[idx].startsWith("--couchport=")) {   
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpswd=")) {
                couchPswd = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchfrom=")) {
                couchFrom = Integer.parseInt(args[idx].substring(12));
            } else if (args[idx].startsWith("--couchto=")) {
                couchTo = Integer.parseInt(args[idx].substring(10));
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
        
        final Couch2Json c2j = new Couch2Json(couchHost,
                                              couchPort,
                                              couchDbName,
                                              couchUser,
                                              couchPswd);
        c2j.setQuery(couchQuery, couchFrom, couchTo);
        
        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new Couch2Lucene(c2j, j2l).export(tell);
    }    
}
