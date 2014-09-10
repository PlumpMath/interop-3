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

package org.bireme.interop.toJson;

import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140812
 */
public class Couch2Json extends ToJson {
    public static int COUCH_DEFAULT_PORT = 5984;
    
    public Couch2Json(final String couchHost,
                      final String couchDb) {
        this(couchHost, Integer.toString(COUCH_DEFAULT_PORT), couchDb, 
                                                                    null, null);
    }
    
    public Couch2Json(final String couchHost,
                      final String couchPort,
                      final String couchDb,
                      final String couchUser,
                      final String couchPswd) {
        if (couchHost == null) {
            throw new NullPointerException("couchHost");
        }
        if (couchPort == null) {
            throw new NullPointerException("couchPort");
        }
        if (couchDb == null) {
            throw new NullPointerException("couchDb");
        }
        
        throw new UnsupportedOperationException("Sorry. Not implemented yet.");
    /*    
        final String usr = (user == null) ? "" : user;
        final String psw = (password == null) ? "" : password;       
        conn = DriverManager.getConnection(url, usr, psw);
        
        final Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        rs.absolute(from);
        
        final ResultSetMetaData rsmd = rs.getMetaData();
        colName = getColName(rsmd);
        next = getNext();
      */
    }
    
    public void setQuery(final String query,
                         final int from,
                         final int to) throws Exception {
        if (query == null) {
            throw new NullPointerException("query");
        }
        if (from < 1) {
            throw new IllegalArgumentException("from[" + from + "] < 1");
        }
        if (to < from) {
            throw new IllegalArgumentException("to[" + to + "] <= from[" + from 
                                                                         + "]");
        }        
        this.to = to;
    }
            
    @Override
    protected JSONObject getNext() {
        Logger.getLogger(this.getClass().getName())
                                        .severe("Operation not yet supported.");
        
        return null;
    }
    
}
