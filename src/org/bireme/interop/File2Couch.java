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

import java.io.IOException;
import org.bireme.interop.fromJson.Json2Couch;
import org.bireme.interop.toJson.File2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140908
 */
public class File2Couch extends Source2Destination {

    public File2Couch(final File2Json f2j, 
                      final Json2Couch j2c) {
        super(f2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: File2Couch <dirpath> <filenameregexp> "
                             + "<couchhost> <couchdb> OPTIONS");
        System.err.println();
        System.err.println("       <dirpath> - Source directory path.");
        System.err.println("       <filenameregexp> - Regular expression describing the file names.");
        System.err.println("       <couchhost> - Destination CouchDB server url.");
        System.err.println("       <couchdb> - Destination CouchDB database.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --encoding=<str>");
        System.err.println("           Source files encoding.");
        System.err.println("       --couchport=<port>");
        System.err.println("           Destination CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           Destination CouchDB server user.");
        System.err.println("       --couchpsw=<password>");  
        System.err.println("           Destination CouchDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --recursive");
        System.err.println("           Search subdirectories for files.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination CouchDB database.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws IOException  {
        final int len = args.length;
        if (len < 4) {
            usage();
        }
        
        final String dirPath = args[0];
        final String fileNameRegExp = args[1];
        final String couchHost = args[2];
        final String couchDbName = args[3];
        
        
        String encoding = File2Json.DEFAULT_ENCODING;
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
                                
        boolean recursive = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 4; idx < len; idx++) {
            if (args[idx].startsWith("--encoding=")) {   
                encoding = args[idx].substring(11);
            } else if (args[idx].startsWith("--couchport=")) {   
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpswd=")) {
                couchPswd = args[idx].substring(12);                
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--recursive")) {
                recursive = true;
            } else if (args[idx].equals("--append")) {
                append = true;    
            } else {
                usage();
            }
        }
        
        final File2Json f2j = new File2Json(dirPath,
                                            fileNameRegExp,
                                            encoding,
                                            recursive);

        final Json2Couch j2c = new Json2Couch(couchHost,
                                              couchPort,
                                              couchDbName,
                                              couchUser,
                                              couchPswd,
                                              append);
        
        new File2Couch(f2j, j2c).export(tell);
    }
}