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
import java.util.HashMap;
import java.util.Map;
import org.bireme.interop.fromJson.Json2Isis;
import org.bireme.interop.toJson.Mongo2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140813
 */
public class Mongo2Isis extends Source2Destination {

    public Mongo2Isis(Mongo2Json m2j, Json2Isis j2i) {
        super(m2j, j2i);
    }
    
    private static void usage() {
        System.err.println("usage: Mongo2Isis <mongohost> <mongodb> <mongocol>"
                                           + " <mongoquery> <mstName> OPTIONS");
        System.err.println();
        System.err.println("       <mongohost> - MongoDB server url.");
        System.err.println("       <mongodb> - MongoDB database.");
        System.err.println("       <mongocol> - MongoDB colection name.");
        System.err.println("       <mongoquery> - Search to retrieve the documents"
                                     + "to be exported from MongoDB database.");
        System.err.println("       <isismst> - Destination Isis master file name.");        
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
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --idtag=<str>");
        System.err.println("           MongoDB field which contains the mfn of "
                                                   + "the destination record.");
        System.err.println("       --convTable=<file>");
        System.err.println("           Convertion table file name used"
                       + " to convert MongoDB field tag into Isis record tag.");
        System.err.println("           One convertion per line of type: " 
                                       + "<mongodbdb_tag_str>=<field_tag_num>");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
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
        if (len < 5) {
            usage();
        }
        
        final String mongoHost = args[0];
        final String mongoDbName = args[1];
        final String mongoColName = args[2];   
        final String mongoQuery = args[3];
        final String mstName = args[4];
        final boolean ffi = true;
        
        String mongoPort = Integer.toString(Mongo2Json.DEFAULT_MONGO_PORT);
        String mongoUser = null;
        String mongoPswd = null;                                      
        int mongoFrom = 1;
        int mongoTo = Integer.MAX_VALUE;
        
        String encoding = Master.DEFAULT_ENCODING;
        String idTag = null;        
        Map<String,Integer> convTable = null;
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
            } else if (args[idx].startsWith("--isisencoding=")) {
                encoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--idtag=")) {
                idTag = args[idx].substring(8);
            } else if (args[idx].startsWith("--convTable=")) {
                convTable = getConvTable(args[idx].substring(12));
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
        
        final Json2Isis j2i = new Json2Isis(mstName,
                                            convTable,
                                            encoding,
                                            idTag,
                                            ffi,
                                            append);
        
        new Mongo2Isis(m2j, j2i).export(tell);
    }
}
