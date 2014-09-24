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

import java.util.Date;
import org.bireme.interop.fromJson.Json2Couch;
import org.bireme.interop.toJson.TweetLoader;
import org.bireme.interop.toJson.Twitter2Json;
import twitter4j.Query;

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
        System.err.println("usage: Twitter2Couch [--twitteruserid=<str>|--twitterquery=<str>]"
                             + "<couchhost> <couchdb> OPTIONS");
        System.err.println();
        System.err.println("       <twitteruserid> - Twitter user id.");
        System.err.println("       <twitterquery> - Twitter query search.");
        System.err.println("       <couchhost> - Destination CouchDB server url.");
        System.err.println("       <couchdb> - Destination CouchDB database.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --twittertotal=<num>");  
        System.err.println("           Max number of retrieved tweets.");
        System.err.println("       --twitterlowerdate=<yyyymmdd>");  
        System.err.println("           Date of the older tweet retrieved.");
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
                
        final String idQuery = args[0];
        final String couchHost = args[1];
        final String couchDbName = args[2];
        
        final String twitterUserId = idQuery.startsWith("--twitteruserid=") 
                                                 ? idQuery.substring(16) : null;
        final Query twitterQuery = idQuery.startsWith("--twitterquery=") 
                                      ? new Query(idQuery.substring(15)) : null;
        int twitterTotal = TweetLoader.MAX_PAGE_SIZE;        
        Date twitterLowerDate = null;
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
                                
        boolean useRetweets = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        if ((twitterUserId == null) && (twitterQuery == null)) {
            usage();
        }
        if ((couchHost == null) && (couchDbName == null)) {
            usage();
        }
        for (int idx = 3; idx < len; idx++) {
            if (args[idx].startsWith("--twittertotal=")) {   
                twitterTotal = Integer.parseInt(args[idx].substring(15));
            } else if (args[idx].startsWith("--twitterlowerdate=")) {   
                twitterLowerDate = new Date(args[idx].substring(19));
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
                                                  twitterQuery,
                                                  twitterTotal,
                                                  twitterLowerDate,
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
