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

import bruma.BrumaException;
import bruma.master.Master;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.bireme.interop.fromJson.Json2File;
import org.bireme.interop.toJson.Isis2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140918
 */
public class Isis2File extends Source2Destination {

    public Isis2File(final Isis2Json i2j, 
                     final Json2File j2f) {
        super(i2j, j2f);
    }
    
    private static void usage() {
        System.err.println("usage: Isis2File <isismst> <outfile> OPTIONS");
        System.err.println();
        System.err.println("       <isismst> - Isis master file name.");
        System.err.println("       <outfile> - Output Json file name (if only one file");
        System.err.println("                   or outfile_<num> if multiples output files).");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --isistags=<num>,<num>,<num>,...");
        System.err.println("           Isis field tags to be exported (comma separated).");
        System.err.println("       --isisfrom=<mfn>");
        System.err.println("           Initial mfn to be exported.");
        System.err.println("       --isisto=<mfn>");
        System.err.println("           Last mfn to be exported.");
        System.err.println("       --outencoding=<encod>");
        System.err.println("           Encoding of the csv file.");
        System.err.println("       --convtable=<file>");
        System.err.println("           Convertion table file name used "
                      + "to convert Isis record tag into CouchDB field tag.");
        System.err.println("           One convertion per line of type: " 
                                         + "<field_tag_num>=<couchdb_tag_str>");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --onefileonly");
        System.err.println("           All records into only one output file.");
                
        System.exit(1);
    }

        public static void main(final String[] args) throws BrumaException,
                                                                   IOException {
        final int len = args.length;
        if (len < 2) {
            usage();
        }
        
        final String mstname = args[0];
        final String csvFile = args[1];
        
        String isisEncoding = Master.GUESS_ISO_IBM_ENCODING;
        List<Integer> tags = null;
        int from = 1;
        int to = Integer.MAX_VALUE;

        String fileEncoding = Json2File.DEFAULT_ENCODING;
        String convTable = null;
        
        boolean oneFileOnly = false;
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 2; idx < len; idx++) {
            if (args[idx].startsWith("--isisencoding=")) {
                isisEncoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--isistags=")) {
                final String stags = args[idx].substring(11);
                tags = new ArrayList<>();
                for (String stag : stags.split("=")) {
                    tags.add(Integer.parseInt(stag));
                }
            } else if (args[idx].startsWith("--isisfrom=")) {
                from = Integer.parseInt(args[idx].substring(11));
            } else if (args[idx].startsWith("--isisto=")) {
                to = Integer.parseInt(args[idx].substring(9));
            } else if (args[idx].startsWith("--fileencoding=")) {
                fileEncoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--convtable=")) {
                convTable = args[idx].substring(12);
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--onefileonly")) {
                oneFileOnly = true;     
            } else {
                usage();
            }
        }
        
        final Isis2Json i2j = new Isis2Json(mstname,
                                            isisEncoding,
                                            tags,
                                            from,
                                            to,
                                            convTable);
        final Json2File j2f = new Json2File(csvFile,
                                            fileEncoding,
                                            oneFileOnly);
        
        new Isis2File(i2j, j2f).export(tell);
    }
}
