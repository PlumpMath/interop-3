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

import org.bireme.interop.fromJson.FromJson;
import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.ToJson;
import org.bireme.interop.toJson.Twitter2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140904
 */
public class Twitter2Lucene extends Source2Destination {

    public Twitter2Lucene(final ToJson tj, 
                         final FromJson fj) {
        super(tj, fj);
    }
    
    private static void usage() {
        System.err.println("usage: Twitter2Lucene <twitteruserid> <lucenedir> "
                                                                   + "OPTIONS");
        System.err.println();
        System.err.println("       <twitteruserid> - Twitter user id.");
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --twitterfrom=<num>");  
        System.err.println("           Initial sequential post number (from end).");
        System.err.println("       --twitterto=<num>");  
        System.err.println("           Last sequential post number (from end).");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination Lucene index.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 2) {
            usage();
        }
        
        final String twitterUserId = args[0];
        final String luceneDir = args[1];
        
        int twitterFrom = 1;
        int twitterTo = Twitter2Json.MAX_RATE_LIMIT;                
                                
        boolean useRetweets = false;
        boolean store = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 2; idx < len; idx++) {
            if (args[idx].startsWith("--twitterfrom=")) {   
                twitterFrom = Integer.parseInt(args[idx].substring(14));
            } else if (args[idx].startsWith("--twitterto=")) {
                twitterTo = Integer.parseInt(args[idx].substring(12));
            } else if (args[idx].equals("--store")) {
                store = true;
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

        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new Twitter2Lucene(t2j, j2l).export(tell);
    }
}
