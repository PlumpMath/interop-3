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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140813
 */
public class Mongo2Json extends ToJson {
    public static final int DEFAULT_MONGO_PORT = 27017;
    
    private final DBCollection coll;
    private DBCursor cursor;
    
    public Mongo2Json(final String mongoHost,
                      final String mongoDbName,
                      final String mongoColName) throws UnknownHostException {
        this(mongoHost, Integer.toString(DEFAULT_MONGO_PORT), null, null, 
                                                     mongoDbName, mongoColName);
    }
    
    public Mongo2Json(final String mongoHost,
                      final String mongoPort,
                      final String mongoUser,
                      final String mongoPswd,
                      final String mongoDbName,
                      final String mongoColName) throws UnknownHostException {
        assert mongoHost != null;
        assert mongoPort != null;
        assert mongoDbName != null;
        assert mongoColName != null;

        final MongoClient mongoClient = new MongoClient(mongoHost, 
                                                   Integer.parseInt(mongoPort));
        final DB db = mongoClient.getDB(mongoDbName);
        if ((mongoUser != null) && (!mongoDbName.isEmpty())) {
            if (! db.authenticate(mongoUser, mongoPswd.toCharArray())) {
                throw new IllegalArgumentException("invalid user/password");
            }
        }
        
        coll = db.getCollection(mongoColName); 
        cursor = null;
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
            throw new IllegalArgumentException("to[" + to + "] < from[" + from
                                                                         + "]");
        }
        this.to = to;
        this.from = from;

        final Object obj = JSON.parse(query);
        final DBObject dbObj = (DBObject) obj;

        cursor = coll.find(dbObj).skip(from - 1).limit(to - from + 1);
        next = getNext();
    }
    
    @Override
    protected JSONObject getNext() throws Exception {
        final JSONObject jobj;
        
        if (cursor.hasNext()) {
            jobj = convertToJSONObject(cursor.next());
        } else {
            jobj = null;
        }
        return jobj;
    }
    
    private JSONObject convertToJSONObject(final DBObject dobj) {
        assert dobj != null;
        
        return new JSONObject(dobj.toString());
    }    
}
