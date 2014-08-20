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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

/**
 *  jdbc:mysql://[host:port],[host:port].../[database] »
 *  [?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]...
 * 
 * @author Heitor Barbieri
 * date 20140806
 */
public class SQL2Json extends ToJson {
    private final Connection conn;
    private final ResultSet rs;
    private final List<String> colName;
    
    public SQL2Json(final String prefix,
                    final String host,
                    final String port,
                    final String dbname,
                    final String query) throws SQLException {
        this(prefix, host, port, dbname, query, null, null, 1, 
                                                             Integer.MAX_VALUE);
    }
    
    public SQL2Json(final String prefix, // for ex: jdbc:<prefix>://[host:port],[ho
                    final String host,
                    final String port,
                    final String user,
                    final String password,
                    final String dbname,
                    final String query,
                    final int from,
                    final int to) throws SQLException {
        if (prefix == null) {
            throw new NullPointerException("prefix");
        }
        if (host == null) {
            throw new NullPointerException("host");
        }
        if (dbname == null) {
            throw new NullPointerException("dbname");
        }
        if (query == null) {
            throw new NullPointerException("query");
        }
        if (! query.trim().toUpperCase().startsWith("SELECT")) {
            throw new IllegalArgumentException("query must start with 'SELECT'");
        }
        if (from < 1) {
            throw new IllegalArgumentException("from[" + from + "] < 1");
        }
        if (to < from) {
            throw new IllegalArgumentException("to[" + to + "] <= from[" + from 
                                                                         + "]");
        }        
        this.to = to;
        
        final String prt = (port == null) ? "" : port;
        final String usr = (user == null) ? "" : user;
        final String psw = (password == null) ? "" : password;
        final String url = "jdbc:" + prefix.trim() + "://" + host
                              + (prt.isEmpty() ? "" : ":" + prt) + "/" + dbname;
        conn = DriverManager.getConnection(url, usr, psw);
        
        final Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        rs.absolute(from);
        
        final ResultSetMetaData rsmd = rs.getMetaData();
        colName = getColName(rsmd);
        next = getNext();
    }
    
    private List<String> getColName(final ResultSetMetaData rsmd) 
                                                           throws SQLException {
        assert rsmd != null;
        
        final List<String> lst = new ArrayList<>();
        final int num = rsmd.getColumnCount();
        
        for (int idx = 1; idx <= num; idx++) {
            lst.add(rsmd.getColumnLabel(idx));
        }
        return lst;
    }
        
    @Override
    protected final JSONObject getNext() throws SQLException {
        assert rs != null;
            
        JSONObject obj;
            
        try {
            if (rs.next()) {
                if (rs.getRow() > to) {
                    obj = null;
                    conn.close();
                } else {
                    obj = new JSONObject();
                    for (String cname : colName) {
                        final Object sqlobj = rs.getObject(cname);
                        obj.putOnce(cname, (sqlobj == null) ? "" : sqlobj);
                    }
                }
            } else {
                obj = null;
                conn.close();
            }                        
        } catch (SQLException ex) {
            conn.close();
            throw ex;
        }        
        return obj;
    }    
}
