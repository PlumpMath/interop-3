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

import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.Csv2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140821
 */
public class Csv2Lucene extends Source2Destination {
    
    public Csv2Lucene(final Csv2Json c2j, 
                      final Json2Lucene j2l) {
        super(c2j, j2l);
    }
    
    private static void usage() {
        System.err.println("usage: Csv2Lucene <csvfile> <lucenedir> OPTIONS");
        System.err.println();
        System.err.println("       <csvfile> - Comma separated file name.");
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --csvencoding=<encod>");
        System.err.println("           Csv file encoding. Default is UTF-8.");
        System.err.println("       --csvdelimiter");
        System.err.println("           Csv field delimiter character");          
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --hasheader");
        System.err.println("           Csv first is header.");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");
        System.err.println("           Appends the documents into the destination Lucene index.");
               
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception  {        
        final int len = args.length;
        if (len < 2) {
            usage();
        }
        
        final String csvFile = args[0];       
        final String luceneDir = args[1];
                
        String csvEncoding = "UTF-8";
        String csvDelimiter = Character.toString(Csv2Json.DEF_DELIMITER);               
                   
        boolean hasHeader = false;
        boolean store = false;
        boolean append = false;        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 2; idx < len; idx++) {
            if (args[idx].startsWith("--csvencoding=")) {   
                csvEncoding = args[idx].substring(14);
            } else if (args[idx].startsWith("--csvdelimiter=")) {
                csvDelimiter = args[idx].substring(15);
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--hasheader")) {
                hasHeader = true; 
            } else if (args[idx].equals("--store")) {
                store = true;
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
        
        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new Csv2Lucene(c2j, j2l).export(tell);
    }
}
