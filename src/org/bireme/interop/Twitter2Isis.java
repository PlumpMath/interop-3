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

import bruma.master.Master;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.bireme.interop.fromJson.Json2Isis;
import org.bireme.interop.toJson.TweetLoader;
import org.bireme.interop.toJson.Twitter2Json;
import twitter4j.Query;

/**
 *
 * @author Heitor Barbieri
 * date: 20140904
 */
public class Twitter2Isis extends Source2Destination {

    public Twitter2Isis(final Twitter2Json t2j,
                        final Json2Isis j2i) {
        super(t2j, j2i);
    }
    
    private static void usage() {
        System.err.println("usage: Twitter2Isis [--twitteruserid=<str>|--twitterquery=<str>]"
                                                    + " <isismst> OPTIONS");
        System.err.println();
        System.err.println("       <twitteruserid> - Twitter user id.");
        System.err.println("       <twitterquery> - Twitter query search.");
        System.err.println("       <isismst> - Destination Isis master file name.");        
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --twittertotal=<num>");  
        System.err.println("           Max number of retrieved tweets.");
        System.err.println("       --twitterlowerdate=<yyyymmdd>");  
        System.err.println("           Date of the older tweet retrieved.");
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --convTable=<file>");
        System.err.println("           Convertion table file name used"
                       + " to convert Twitter field tag into Isis record tag.");
        System.err.println("           One convertion per line of type: " 
                                       + "<twitter_tag_str>=<field_tag_num>");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --useretweets");
        System.err.println("           Also exports retweets.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination Isis database.");
               
        System.exit(1);
    }
    
    private static Map<String,Integer> getConvTable(final String in) 
                                                            throws IOException {
        assert in != null;
        
        final Map<String,Integer> map = new HashMap<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(in))) {
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                final String lineT = line.trim();
                if (!lineT.isEmpty()) {
                    final String[] split = lineT.split(" *= *", 2);
                    map.put(split[0], Integer.valueOf(split[1]));
                }
            }
        }
        
        return map;
    }
    
    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 2) {
            usage();
        }
        
        final String idQuery = args[0];
        final String mstName = args[1];
        final boolean ffi = true;
        
        final String twitterUserId = idQuery.startsWith("--twitteruserid=") 
                                                 ? idQuery.substring(16) : null;
        final String twitterQuery = idQuery.startsWith("--twitterquery=") 
                                                 ? idQuery.substring(15) : null;
        int twitterTotal = TweetLoader.MAX_PAGE_SIZE; 
        Date twitterLowerDate = null;
        String encoding = Master.DEFAULT_ENCODING;
        Map<String,Integer> convTable = null;
                                
        boolean useRetweets = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        if ((twitterUserId == null) && (twitterQuery == null)) {
            usage();
        }
        
        for (int idx = 2; idx < len; idx++) {
            if (args[idx].startsWith("--twittertotal=")) {   
                twitterTotal = Integer.parseInt(args[idx].substring(15));
            } else if (args[idx].startsWith("--twitterlowerdate=")) {   
                twitterLowerDate = new Date(args[idx].substring(19));
            } else if (args[idx].startsWith("--isisencoding=")) {
                encoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--convTable=")) {
                convTable = getConvTable(args[idx].substring(12));
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

        final Json2Isis j2i = new Json2Isis(mstName,
                                            convTable,
                                            encoding,
                                            null,
                                            ffi,
                                            append);
        
        new Twitter2Isis(t2j, j2i).export(tell);
    }
}
