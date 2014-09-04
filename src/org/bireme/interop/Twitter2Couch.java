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
import org.bireme.interop.toJson.Twitter2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140904
 */
public class Twitter2Couch extends Source2Destination {

    public Twitter2Couch(final Twitter2Json t2j, 
                         final Json2Couch j2c) {
        super(t2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: Twitter2Couch <twitteruserid> "
                             + "<couchhost> <couchdb> OPTIONS");
        System.err.println();
        System.err.println("       <twitteruserid> - Twitter user id.");
        System.err.println("       <couchhost> - Destination CouchDB server url.");
        System.err.println("       <couchdb> - Destination CouchDB database.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --twitterfrom=<num>");  
        System.err.println("           Initial sequential post number (from end).");
        System.err.println("       --twitterto=<num>");  
        System.err.println("           Last sequential post number (from end).");
        System.err.println("       --couchport=<port>");
        System.err.println("           Destination CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           Destination CouchDB server user.");
        System.err.println("       --couchpsw=<password>");  
        System.err.println("           Destination CouchDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --useretweets");
        System.err.println("           Also exports retweets.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination CouchDB database.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 3) {
            usage();
        }
        
        final String twitterUserId = args[0];
        final String couchHost = args[1];
        final String couchDbName = args[2];
        
        int twitterFrom = 1;
        int twitterTo = Twitter2Json.MAX_RATE_LIMIT;
        
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
                                
        boolean useRetweets = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 3; idx < len; idx++) {
            if (args[idx].startsWith("--twitterfrom=")) {   
                twitterFrom = Integer.parseInt(args[idx].substring(14));
            } else if (args[idx].startsWith("--twitterto=")) {
                twitterTo = Integer.parseInt(args[idx].substring(12));
            } else if (args[idx].startsWith("--couchport=")) {   
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpswd=")) {
                couchPswd = args[idx].substring(12);                
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--useretweets")) {
                useRetweets = true;
            } else if (args[idx].equals("--append")) {
                append = true;
            } else {
                usage();
            }
        }
        
        final Twitter2Json t2j = new Twitter2Json(twitterUserId,
                                                  twitterFrom,
                                                  twitterTo,
                                                  useRetweets);

        final Json2Couch j2c = new Json2Couch(couchHost,
                                              couchPort,                                              
                                              couchUser,
                                              couchPswd,
                                              couchDbName,
                                              append);
        
        new Twitter2Couch(t2j, j2c).export(tell);
    }
}
