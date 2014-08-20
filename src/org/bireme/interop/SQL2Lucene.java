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
import java.sql.SQLException;
import org.bireme.interop.fromJson.Json2Lucene;
import org.bireme.interop.toJson.SQL2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140815
 */
public class SQL2Lucene extends Source2Destination {

    public SQL2Lucene(final SQL2Json s2j, 
                      final Json2Lucene j2l) {
        super(s2j, j2l);
    }
    
    private static void usage() {
        System.err.println("usage: SQL2Lucene <sqlprefix> <sqlhost> <sqlport> "
                                + "<sqldbname> <sqlquery> <lucenedir> OPTIONS");
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
        System.err.println("       <lucenedir> - Destination Lucene index directory.");
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
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --store");  
        System.err.println("           Stores the documents into the destination Lucene index.");
        System.err.println("       --append");        
        System.err.println("           Appends the documents into the destination Lucene index.");
        
        System.exit(1);
    }
    
    public static void main(final String[] args) throws SQLException, 
                                                        IOException {
        final int len = args.length;
        if (len < 6) {
            usage();
        }
        
        final String sqlPrefix = args[0];  
        final String sqlHost = args[1];  
        final String sqlPort = args[2];
        final String sqlDbName = args[3];
        final String sqlQuery = args[4];
        final String luceneDir = args[5];
                
        String sqlUser = null;
        String sqlPswd = null;
        int sqlFrom = 1;
        int sqlTo = Integer.MAX_VALUE;
                            
        boolean store = false;
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
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--store")) {
                store = true;    
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
        
        final Json2Lucene j2l = new Json2Lucene(luceneDir,
                                                store,
                                                append);
        
        new SQL2Lucene(s2j, j2l).export(tell);
    }
}
