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
import org.bireme.interop.toJson.Csv2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140821
 */
public class Csv2Couch extends Source2Destination {

    public Csv2Couch(final Csv2Json c2j, 
                     final Json2Couch j2c) {
        super(c2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: Csv2Couch <csvfile> <couchhost> <couchdb> "
                                                                   + "OPTIONS");
        System.err.println();
        System.err.println("       <csvfile> - Comma separated file name.");
        System.err.println("       <couchhost> - CouchDB server url.");
        System.err.println("       <couchdb> - CouchDB database.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --csvencoding=<encod>");
        System.err.println("           Csv file encoding. Default is UTF-8.");
        System.err.println("       --csvdelimiter");
        System.err.println("           Csv field delimiter character");        
        System.err.println("       --couchport=<port>");
        System.err.println("           CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           CouchDB server user.");
        System.err.println("       --couchpsw=<password>");  
        System.err.println("           CouchDB server *password.");        
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --hasheader");
        System.err.println("           Csv first is header.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination CouchDB database.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception {
        final int len = args.length;
        if (len < 3) {
            usage();
        }

        final String csvFile = args[0];
        final String couchHost = args[1];
        final String couchDbName = args[2];
        
        String csvEncoding = "UTF-8";
        String csvDelimiter = Character.toString(Csv2Json.DEF_DELIMITER);
        
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
                                 
        boolean hasHeader = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 3; idx < len; idx++) {
            if (args[idx].startsWith("--csvencoding=")) {   
                csvEncoding = args[idx].substring(14);
            } else if (args[idx].startsWith("--csvdelimiter=")) {
                csvDelimiter = args[idx].substring(15);
            } else if (args[idx].startsWith("--couchport=")) {   
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpswd=")) {
                couchPswd = args[idx].substring(12);                
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--hasheader")) {
                 hasHeader = true;            
            } else if (args[idx].equals("--append")) {
                append = true;
            } else {
                usage();
            }
        }
        
        final Csv2Json c2j = new Csv2Json(csvFile,
                                          csvEncoding,
                                          csvDelimiter.charAt(0),
                                          hasHeader);

        final Json2Couch j2c = new Json2Couch(couchHost,
                                              couchPort,
                                              couchDbName,
                                              couchUser,
                                              couchPswd,
                                              append);
        
        new Csv2Couch(c2j, j2c).export(tell);
    }    
}