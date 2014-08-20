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
import org.bireme.interop.toJson.Couch2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140815
 */
public class Couch2Couch extends Source2Destination {

    public Couch2Couch(final Couch2Json c2j, 
                       final Json2Couch j2c) {
        super(c2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: Couch2Couch <couchhost1> <couchdb1> "
                             + "<couchquery1> <couchhost2> <couchdb2> OPTIONS");
        System.err.println();
        System.err.println("       <couchhost1> - Source CouchDB server url.");
        System.err.println("       <couchdb1> - Source CouchDB database.");
        System.err.println("       <couchquery1> - Search to retrieve the documents"
                               + "to be exported from source CouchDB database.");
        System.err.println("       <couchhost2> - Destination CouchDB server url.");
        System.err.println("       <couchdb1> - Destination CouchDB database.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --couchport1=<port>");
        System.err.println("           Source CouchDB server port.");
        System.err.println("       --couchuser1=<user>");
        System.err.println("           Source CouchDB server user.");
        System.err.println("       --couchpsw1=<password>");  
        System.err.println("           Source CouchDB server password.");
        System.err.println("       --couchfrom1=<num>");  
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --couchto1=<num>");  
        System.err.println("           Last sequential response hit number.");
        System.err.println("       --couchport2=<port>");
        System.err.println("           Destination CouchDB server port.");
        System.err.println("       --couchuser2=<user>");
        System.err.println("           Destination CouchDB server user.");
        System.err.println("       --couchpsw2=<password>");  
        System.err.println("           Destination CouchDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination CouchDB database.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 5) {
            usage();
        }
        
        final String couchHost1 = args[0];
        final String couchDbName1 = args[1];
        final String couchQuery = args[2];
        final String couchHost2 = args[3];
        final String couchDbName2 = args[4];
        
        String couchPort1 = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchPort2 = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser1 = null;
        String couchUser2 = null;
        String couchPswd1 = null;
        String couchPswd2 = null;
        int couchFrom = 1;
        int couchTo = Integer.MAX_VALUE;
                                        
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 5; idx < len; idx++) {
            if (args[idx].startsWith("--couchport1=")) {   
                couchPort1 = args[idx].substring(13);
            } else if (args[idx].startsWith("--couchuser1=")) {
                couchUser1 = args[idx].substring(13);
            } else if (args[idx].startsWith("--couchpswd1=")) {
                couchPswd1 = args[idx].substring(13);
            } else if (args[idx].startsWith("--couchport2=")) {   
                couchPort2 = args[idx].substring(13);
            } else if (args[idx].startsWith("--couchuser2=")) {
                couchUser2 = args[idx].substring(13);
            } else if (args[idx].startsWith("--couchpswd2=")) {
                couchPswd2 = args[idx].substring(13);                
            } else if (args[idx].startsWith("--couchfrom1=")) {
                couchFrom = Integer.parseInt(args[idx].substring(13));
            } else if (args[idx].startsWith("--couchto1=")) {
                couchTo = Integer.parseInt(args[idx].substring(11));
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--append")) {
                append = true;
            } else {
                usage();
            }
        }
        
        final Couch2Json c2j = new Couch2Json(couchHost1,
                                              couchPort1,
                                              couchDbName1,
                                              couchUser1,
                                              couchPswd1);
        c2j.setQuery(couchQuery, couchFrom, couchTo);

        final Json2Couch j2c = new Json2Couch(couchHost2,
                                              couchPort2,
                                              couchDbName2,
                                              couchUser2,
                                              couchPswd2,
                                              append);
        
        new Couch2Couch(c2j, j2c).export(tell);
    }
}
