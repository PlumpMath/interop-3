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

import bruma.BrumaException;
import bruma.master.Field;
import bruma.master.Master;
import bruma.master.MasterFactory;
import bruma.master.Record;
import bruma.master.Subfield;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140812
 */
public class Json2Isis implements FromJson {
    final String mstName;
    final Map<String,Integer> convTable;
    final String encoding;
    final String idTag;
    final boolean ffi;
    final boolean append;
    
    int lastTag;
    
    public Json2Isis(final String mstName,
                     final Map<String,Integer> convTable) {
        this(mstName, convTable, Master.DEFAULT_ENCODING, null, true, false);
    }
    
    public Json2Isis(final String mstName,
                     final Map<String,Integer> convTable,
                     final String encoding,
                     final String idTag,
                     final boolean ffi,
                     final boolean append) {
        if (mstName == null) {
            throw new NullPointerException("mstName");
        }
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
        this.mstName = mstName;
        this.convTable = (convTable == null) ? new HashMap<String,Integer>() 
                                             : convTable;                
        this.encoding = encoding;
        this.idTag = idTag;
        this.ffi = ffi;
        this.append = append;
        lastTag = 0;
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

        try {
            final MasterFactory minterf = MasterFactory.getInstance(mstName)
                                                       .setEncoding(encoding)
                                                       .setFFI(ffi);
            final Master mst = (Master) (append ? minterf.open() 
                                                : minterf.forceCreate());
            int tot = 0;
            
            for (JSONObject obj : it) {
                if (++tot % tell == 0) {
                    System.out.println("+++" + tot);
                }
                mst.writeRecord(convertToRecord(obj));
            }            
            mst.close();
        } catch (BrumaException ex) {
            Logger.getLogger(Json2Isis.class.getName())
                                                   .log(Level.SEVERE, null, ex);
        }
    }
    
    private Record convertToRecord(JSONObject jobj) throws BrumaException {
        assert jobj != null;
        
        final Record rec = new Record();
        
        if ((idTag != null) && (jobj.has(idTag))) {
            rec.setMfn(jobj.getInt(idTag));
        }
        
        for (String key : jobj.keySet()) {
            final Integer tag = convTable.get(key);
            final int tag2;
            
            if (tag == null) {
                tag2 = ++lastTag;
                convTable.put(key, tag2);
            } else {
                tag2 = tag;
            }
            
            final Object obj = jobj.get(key);

            if (obj instanceof JSONArray) {
                final JSONArray array = (JSONArray)obj;
                final int len = array.length();

                for (int idx = 0; idx < len; idx++) {
                    final Object obj2 = array.get(idx);
                    
                    if (obj2 instanceof JSONArray) {
                        final List<Subfield> subflds = new ArrayList<>();
                        final JSONArray array2 = (JSONArray)obj2;
                        final int len2 = array2.length();
                        
                        for (int idx2 = 0; idx2 < len2; idx2++) {
                            final Object obj3 = array2.get(idx2);
                            subflds.add(new Subfield((char)('a' + idx2), 
                                                     obj3.toString()));
                        }
                        rec.addField(new Field(tag2, subflds));
                    } else {
                        rec.addField(tag2, obj2.toString());
                    }
                }
            } else {
                rec.addField(tag2, obj.toString());
            }
        }        
        
        return rec;
    }
}
