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
import org.bireme.interop.fromJson.Json2Mongo;
import org.bireme.interop.toJson.TweetLoader;
import org.bireme.interop.toJson.Twitter2Json;
import twitter4j.Query;

/**
 *
 * @author Heitor Barbieri
 * date: 20140904
 */
public class Twitter2Mongo extends Source2Destination {

    public Twitter2Mongo(final Twitter2Json t2j,
                         final Json2Mongo j2m) {
        super(t2j, j2m);
    }

    private static void usage() {
        System.err.println("usage: Twitter2Mongo [--twitteruserid=<str>|--twitterquery=<str>]"
                             + "<mongohost> <mongodb> <mongocol> OPTIONS");
        System.err.println();
        System.err.println("       <twitteruserid> - Twitter user id.");
        System.err.println("       <twitterquery> - Twitter query search.");
        System.err.println("       <mongohost> - MongoDB server url.");
        System.err.println("       <mongodb> - MongoDB database.");
        System.err.println("       <mongocol> - MongoDB colection name.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --twittertotal=<num>");  
        System.err.println("           Max number of retrieved tweets.");
        System.err.println("       --twitterlowerdate=<yyyymmdd>");  
        System.err.println("           Date of the older tweet retrieved.");
        System.err.println("       --mongoport=<port>");
        System.err.println("           MongoDb server port.");
        System.err.println("       --mongouser=<user>");
        System.err.println("           MongoDB server user.");
        System.err.println("       --mongopsw=<password>");
        System.err.println("           MongoDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --useretweets");
        System.err.println("           Also exports retweets.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination MongoDB database.");

        System.exit(1);
    }

    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 4) {
            usage();
        }

        final String idQuery = args[0];
        final String mongoHost = args[1];
        final String mongoDbName = args[2];
        final String mongoColName = args[3];

        final String twitterUserId = idQuery.startsWith("--twitteruserid=") 
                                                 ? idQuery.substring(16) : null;
        final String twitterQuery = idQuery.startsWith("--twitterquery=") 
                                                 ? idQuery.substring(15) : null;
        int twitterTotal = TweetLoader.MAX_PAGE_SIZE;        
        Date twitterLowerDate = null;

        String mongoPort = Integer.toString(Json2Mongo.DEFAULT_MONGO_PORT);
        String mongoUser = null;
        String mongoPswd = null;

        boolean useRetweets = false;
        boolean append = false;

        int tell = Integer.MAX_VALUE;

        if ((twitterUserId == null) && (twitterQuery == null)) {
            usage();
        }
        
        for (int idx = 4; idx < len; idx++) {
            if (args[idx].startsWith("--twittertotal=")) {   
                twitterTotal = Integer.parseInt(args[idx].substring(15));
            } else if (args[idx].startsWith("--twitterlowerdate=")) {   
                twitterLowerDate = new Date(args[idx].substring(19));
            } else if (args[idx].startsWith("--mongoport=")) {
                mongoPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--mongouser=")) {
                mongoUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--mongopsw=")) {
                mongoPswd = args[idx].substring(12);
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
                                                  new Query(twitterQuery),
                                                  twitterTotal,
                                                  twitterLowerDate,
                                                  useRetweets);

        final Json2Mongo j2m = new Json2Mongo(mongoHost,
                                              mongoPort,
                                              mongoUser,
                                              mongoPswd,
                                              mongoDbName,
                                              mongoColName,
                                              append);

        new Twitter2Mongo(t2j, j2m).export(tell);
    }
}
