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
import org.bireme.interop.toJson.File2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140908
 */
public class File2Isis extends Source2Destination {

    public File2Isis(final File2Json f2j, 
                     final Json2Isis j2i) {
        super(f2j, j2i);
    }
    
    private static void usage() {
        System.err.println("usage: File2Isis <dirpath> <filenameregexp> "
                                                         + "<isismst> OPTIONS");
        System.err.println();
        System.err.println("       <dirpath> - Source directory path.");
        System.err.println("       <filenameregexp> - Regular expression describing the file names.");
        System.err.println("       <isismst> - Destination Isis master file name.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --encoding=<str>");
        System.err.println("           Source files encoding.");
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --idtag=<str>");        
        System.err.println("           File json field which contains the mfn of "
                                                   + "the destination record.");
        System.err.println("       --convTable=<file>");
        System.err.println("           Convertion table file name used"
                      + " to convert Csv field tag into Isis record tag.");
        System.err.println("           One convertion per line of type: " 
                                         + "<file_tag_str>=<field_tag_num>");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --recursive");
        System.err.println("           Search subdirectories for files.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination CouchDB database.");
               
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
    
    public static void main(final String[] args) throws IOException  {
        final int len = args.length;
        if (len < 3) {
            usage();
        }
        
        final String dirPath = args[0];
        final String fileNameRegExp = args[1];
        final String mstName = args[2];        
                                
        String encoding = File2Json.DEFAULT_ENCODING;
        String isisEncoding = Master.GUESS_ISO_IBM_ENCODING;
        String idTag = null;
        Map<String, Integer> convTable = null;
                
        final boolean ffi = true;
        boolean recursive = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 3; idx < len; idx++) {
            if (args[idx].startsWith("--encoding=")) {   
                encoding = args[idx].substring(11);
            } else if (args[idx].startsWith("--isisencoding=")) {
                isisEncoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--idtag=")) {
                idTag = args[idx].substring(8);
            } else if (args[idx].startsWith("--convTable=")) {
                convTable = getConvTable(args[idx].substring(12));
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

        final Json2Isis j2i = new Json2Isis(mstName,
                                            convTable,
                                            isisEncoding,
                                            idTag,
                                            ffi,
                                            append);
        
        new File2Isis(f2j, j2i).export(tell);
    }
}
