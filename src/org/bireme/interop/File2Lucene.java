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
import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.File2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140909
 */
public class File2Lucene extends Source2Destination {

    public File2Lucene(final File2Json f2j, 
                       final Json2Lucene j2l) {
        super(f2j, j2l);
    }
    
    private static void usage() {
        System.err.println("usage: File2Lucene <dirpath> <filenameregexp> "
                                                       + "<lucenedir> OPTIONS");
        System.err.println();
        System.err.println("       <dirpath> - Source directory path.");
        System.err.println("       <filenameregexp> - Regular expression describing the file names.");
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --encoding=<str>");
        System.err.println("           Source files encoding.");        
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --recursive");
        System.err.println("           Search subdirectories for files.");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination Lucene index.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws IOException  {
        final int len = args.length;
        if (len < 3) {
            usage();
        }
        
        final String dirPath = args[0];
        final String fileNameRegExp = args[1];
        final String luceneDir = args[2];
                                
        String encoding = File2Json.DEFAULT_ENCODING;        
        boolean recursive = false;
        boolean store = false;
        boolean append = false;
        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 3; idx < len; idx++) {
            if (args[idx].startsWith("--encoding=")) {   
                encoding = args[idx].substring(11);
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--recursive")) {
                recursive = true;
            } else if (args[idx].equals("--store")) {
                store = true;
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

        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new File2Lucene(f2j, j2l).export(tell);
    }
}
