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
import org.bireme.interop.fromJson.Json2Couch;
import org.bireme.interop.fromJson.Json2Isis;
import org.bireme.interop.toJson.Couch2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140812
 */
public class Couch2Isis extends Source2Destination {

    public Couch2Isis(final Couch2Json c2j, 
                      final Json2Isis j2i) {
        super(c2j, j2i);
    }
    
    private static void usage() {
        System.err.println("usage: Couch2Isis <couchhost> <couchdb> "
                                            + "<couchquery> <isismst> OPTIONS");
        System.err.println();
        System.err.println("       <couchhost> - CouchDB server url.");
        System.err.println("       <couchdb> - CouchDB database.");
        System.err.println("       <couchquery> - Search to retrieve the documents"
                                     + "to be exported from CouchDB database.");
        System.err.println("       <isismst> - Destination Isis master file name.");
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
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --idtag=<str>");        
        System.err.println("           CouchDB field which contains the mfn of "
                                                   + "the destination record.");
        System.err.println("       --convTable=<file>");
        System.err.println("           Convertion table file name used"
                      + " to convert CouchDB field tag into Isis record tag.");
        System.err.println("           One convertion per line of type: " 
                                         + "<couchdb_tag_str>=<field_tag_num>");
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
        if (len < 4) {
            usage();
        }
        
        final String couchHost = args[0];
        final String couchDbName = args[1];
        final String couchQuery = args[2];
        final String mstName = args[3];        
        
        final boolean ffi = true;
        
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
        int couchFrom = 1;
        int couchTo = Integer.MAX_VALUE;
        
        String encoding = Master.GUESS_ISO_IBM_ENCODING;
        String idTag = null;
        Map<String, Integer> convTable = null;
                                
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
        
        final Couch2Json c2j = new Couch2Json(couchHost,
                                              couchPort,
                                              couchDbName,
                                              couchUser,
                                              couchPswd);
        c2j.setQuery(couchQuery, couchFrom, couchTo);

        final Json2Isis j2i = new Json2Isis(mstName,
                                            convTable,
                                            encoding,
                                            idTag,
                                            ffi,
                                            append);
        
        new Couch2Isis(c2j, j2i).export(tell);
    }
}
