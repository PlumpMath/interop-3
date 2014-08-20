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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.bireme.interop.fromJson.Json2Isis;
import org.bireme.interop.toJson.SQL2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class SQL2Isis extends Source2Destination {

    public SQL2Isis(final SQL2Json s2j, 
                    final Json2Isis j2i) {
        super(s2j, j2i);
    }
    
    private static void usage() {
        System.err.println("usage: SQL2Isis <sqlprefix> <sqlhost> <sqlport> "
                                  + "<sqldbname> <sqlquery> <isismst> OPTIONS");
        System.err.println();
        System.err.println("       <sqlprefix> - jdbc:<sqlprefix>://[host:port]/database");
        System.err.println("           MariaDB = mysql");
        System.err.println("           MySQL = mysql");
        System.err.println("           PostgreSQL = postgresql");
        System.err.println("       <sqlhost> - Source relational database server url.");       
        System.err.println("       <sqlport> - Source relational database server port.");
        System.err.println("           MariaDB = 3306");
        System.err.println("           MySQL = 3306");
        System.err.println("           PostgreSQL = 5432");
        System.err.println("       <sqlquery> - SELECT sql query to retrieve the documents"
                                     + "to be exported from relational database.");
        System.err.println("       <isismst> - Destination Isis master file name.");        
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --sqluser=<usr>");
        System.err.println("           Relational server password.");
        System.err.println("       --sqlpsw=<psw>");
        System.err.println("           Relational server password.");
        System.err.println("       --sqlfrom=<num>");
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --sqlto=<num>");
        System.err.println("           Last sequential response hit number.");        
        System.err.println("       --isisencoding=<encod>");
        System.err.println("           Encoding of the isis records.");
        System.err.println("       --idtag=<str>");
        System.err.println("           Table field which contains the mfn of "
                                                   + "the destination record.");
        System.err.println("       --convTable=<file>");
        System.err.println("           Convertion table file name used"
                         + " to convert table column tag into Isis record tag.");
        System.err.println("           One convertion per line of type: " 
                                          + "<column_tag_str>=<field_tag_num>");
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
    
    public static void main(final String[] args) throws IOException, 
                                                        SQLException {
        final int len = args.length;
        if (len < 6) {
            usage();
        }
        
        final String sqlPrefix = args[0];  
        final String sqlHost = args[1];  
        final String sqlPort = args[2];
        final String sqlDbName = args[3];
        final String sqlQuery = args[4];
        final String mstName = args[5];
        
        String sqlUser = null;
        String sqlPswd = null;
        int sqlFrom = 1;
        int sqlTo = Integer.MAX_VALUE;
                                    
        String encoding = Master.DEFAULT_ENCODING;
        String idTag = null;
        Map<String,Integer> convTable = null;
        boolean ffi = true;
        boolean append = false;
                              
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 6; idx < len; idx++) {
            if (args[idx].startsWith("--sqluser=")) {
                sqlUser = args[idx].substring(10);
            } else if (args[idx].startsWith("--sqlpsw=")) {
                sqlPswd = args[idx].substring(9); 
            } else if (args[idx].startsWith("--sqlfrom=")) {
                sqlFrom = Integer.parseInt(args[idx].substring(10)); 
            } else if (args[idx].startsWith("--sqlto=")) {
                sqlTo = Integer.parseInt(args[idx].substring(8));     
            } else if (args[idx].startsWith("--isisencoding=")) {
                encoding = args[idx].substring(15);
            } else if (args[idx].startsWith("--idtag=")) {
                idTag = args[idx].substring(8);
            } else if (args[idx].startsWith("--convtable=")) {
                convTable = getConvTable(args[idx].substring(12));                
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--append")) {
                append = true;
            } else { 
                usage();
            }
        }
        
        final SQL2Json s2j = new SQL2Json(sqlPrefix,
                                          sqlHost,                                          
                                          sqlPort,
                                          sqlUser,
                                          sqlPswd,
                                          sqlDbName,
                                          sqlQuery,
                                          sqlFrom,
                                          sqlTo);
        
        final Json2Isis j2i = new Json2Isis(mstName,
                                            convTable,
                                            encoding,
                                            idTag,
                                            ffi,
                                            append);
        
        new SQL2Isis(s2j, j2i).export(tell);
    }
}
