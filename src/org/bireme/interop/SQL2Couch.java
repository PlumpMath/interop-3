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
import org.bireme.interop.fromJson.Json2Couch;
import org.bireme.interop.toJson.SQL2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class SQL2Couch extends Source2Destination {

    public SQL2Couch(SQL2Json s2j, Json2Couch j2c) {
        super(s2j, j2c);
    }
    
    private static void usage() {
        System.err.println("usage: SQL2Couch <sqlprefix> <sqlhost> <sqlport> "
                      + "<sqldbname> <sqlquery> <couchhost> <couchdb> OPTIONS");
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
        System.err.println("       <couchhost> - CouchDB server url.");
        System.err.println("       <couchdb> - CouchDB database.");        
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --sqluser=<usr>");
        System.err.println("           Relational server user.");
        System.err.println("       --sqlpsw=<psw>");
        System.err.println("           Relational server password.");
        System.err.println("       --sqlfrom=<num>");
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --sqlto=<num>");        
        System.err.println("           Last sequential response hit number.");
        System.err.println("       --couchport=<port>");
        System.err.println("           CouchDB server port.");
        System.err.println("       --couchuser=<user>");
        System.err.println("           CouchDB server user.");
        System.err.println("       --couchpsw=<password>");   
        System.err.println("           CouchDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --append");        
        System.err.println("           Appends the documents into the destination CouchDB database.");
        
        System.exit(1);
    }
    
    public static void main(final String[] args) throws SQLException, 
                                                        IOException {
        final int len = args.length;
        if (len < 7) {
            usage();
        }
        
        final String sqlPrefix = args[0];  
        final String sqlHost = args[1];  
        final String sqlPort = args[2];
        final String sqlDbName = args[3];
        final String sqlQuery = args[4];
        final String couchHost = args[5];        
        final String couchDbName = args[6];
        
        String sqlUser = null;
        String sqlPswd = null;        
        int sqlFrom = 1;
        int sqlTo = Integer.MAX_VALUE;
                                    
        String couchPort = Integer.toString(Json2Couch.DEFAULT_COUCH_PORT);
        String couchUser = null;
        String couchPswd = null;
        boolean append = false;
                              
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 7; idx < len; idx++) {
            if (args[idx].startsWith("--sqluser=")) {
                sqlUser = args[idx].substring(10);
            } else if (args[idx].startsWith("--sqlpsw=")) {
                sqlPswd = args[idx].substring(9); 
            } else if (args[idx].startsWith("--sqlfrom=")) {
                sqlFrom = Integer.parseInt(args[idx].substring(10)); 
            } else if (args[idx].startsWith("--sqlto=")) {
                sqlTo = Integer.parseInt(args[idx].substring(8));     
            } else if (args[idx].startsWith("--couchport=")) {
                couchPort = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchuser=")) {
                couchUser = args[idx].substring(12);
            } else if (args[idx].startsWith("--couchpsw=")) {
                couchPswd = args[idx].substring(11); 
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
        
        final Json2Couch j2c = new Json2Couch(couchHost,
                                              couchPort,
                                              couchUser,
                                              couchPswd,
                                              couchDbName,
                                              append);
        
        new SQL2Couch(s2j, j2c).export(tell);
    }
}
