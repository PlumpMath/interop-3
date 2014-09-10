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

package org.bireme.interop.fromJson;

import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date 20140811
 */
public class Json2Mongo implements FromJson {

    public static final int DEFAULT_MONGO_PORT = 27017;
    
    private static final int MAX_BULK_DOCS = 10000;

    private final DBCollection coll;

    public Json2Mongo(final String mongoHost,
                      final String mongoDbName,
                      final String mongoColName,
                      final boolean append) throws UnknownHostException {                 
        this (mongoHost, Integer.toString(DEFAULT_MONGO_PORT), null, null,
                                             mongoDbName, mongoColName, append);
    }
    
    public Json2Mongo(final String mongoHost,
                      final String mongoPort,
                      final String mongoUser,
                      final String mongoPswd,
                      final String mongoDbName,
                      final String mongoColName,
                      final boolean append) throws UnknownHostException {
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
        if (!append) {
           coll.drop();
           coll.dropIndexes();                      
        }
    }

    @Override
    public void exportDocuments(final Iterable<JSONObject> it, 
                                final int tell) {
        if (it == null) {
            throw new NullPointerException("it");
        }
        if (tell <= 0) {
            throw new IllegalArgumentException("tell <= 0");
        }
        BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
                                     //coll.initializeUnorderedBulkOperation();
        int tot = 0;
        int bulkNum = 0;
        
        for (JSONObject obj : it) {           
            if (++tot % tell == 0) {
                System.out.println("+++" + tot);
            }
            try {
                coll.insert(convertToDBObj(obj), WriteConcern.SAFE);
            } catch(IllegalArgumentException iae) {
                Logger.getLogger(this.getClass().getName())
                                                     .severe(iae.getMessage());
            }
        }
        
        /*for (JSONObject obj : it) {           
            if (++tot % tell == 0) {
                System.out.println("+++" + tot);
            }
            
            bulkNum++;
            builder.insert(convertToDBObj(obj));
            
            if (bulkNum >= MAX_BULK_DOCS) {     
                System.out.print("Sending documents to MongoDB - ");
                final BulkWriteResult result = builder.execute();
                final int inserted = result.getInsertedCount();
                System.out.println("OK");
                if (inserted < bulkNum) {
                    final String msg = "Insertion error: inserted[" + inserted
                             + "] expected[" + bulkNum + "]";
                    Logger.getLogger(Json2Mongo.class.getName())
                                                        .log(Level.SEVERE, msg);
                }
                bulkNum = 0;
                builder = coll.initializeOrderedBulkOperation();
                //builder = coll.initializeUnorderedBulkOperation();
            }
        }
        if (bulkNum > 0) {
            final BulkWriteResult result = builder.execute();
            final int inserted = result.getInsertedCount();
            if (inserted < bulkNum) {
                final String msg = "Insertion error: inserted[" + inserted
                             + "] expected[" + bulkNum + "]";
                Logger.getLogger(Json2Mongo.class.getName())
                                                        .log(Level.SEVERE, msg);
            }
        }*/
    }    
    
    private DBObject convertToDBObj(final JSONObject jobj) {
        assert jobj != null;
        
        return (DBObject) JSON.parse(jobj.toString());
    }
}
