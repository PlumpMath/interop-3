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

import bruma.BrumaException;
import bruma.master.Field;
import bruma.master.Master;
import bruma.master.MasterFactory;
import bruma.master.Record;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 *
 * @author Heitor Barbieri
 * date 20140807
 */
public class Isis2Json extends ToJson {
    private final Master mst;
    private final List<Integer> tags;
    private final Map<Integer,String> convTable;
    
    int current;
    
    public Isis2Json(final String mstname) throws BrumaException, IOException {
        this(mstname, Master.GUESS_ISO_IBM_ENCODING, null, 1, Integer.MAX_VALUE
                                                                        , null);
    }
            
    public Isis2Json(final String mstname,
                     final String encoding,
                     final List<Integer> tags,
                     final int from,
                     final int to,
                     final String convTableFile) throws BrumaException, 
                                                        IOException {
        if (mstname == null) {
            throw new NullPointerException("mstname");
        }        
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
        if (from < 1) {
            throw new IllegalArgumentException("from[" + from + "] < 1");
        }
        if (to < from) {
            throw new IllegalArgumentException("to[" + to + "] <= from[" + from 
                                                                         + "]");
        }        
        this.from = from;
        this.to = to;
        this.tags = tags;
        this.convTable = (convTableFile == null) ? null 
                                                 : getConvTable(convTableFile);
        this.mst = MasterFactory.getInstance(mstname).setEncoding(encoding)
                                                                        .open();        
        current = from;
        next = getNext();
    }
    
    private JSONObject getRecJson(final Record rec, 
                                  final List<Integer> tags) 
                                                         throws BrumaException {
        assert rec != null;
        
        final Record rec2;
        
        if (tags == null) {
            rec2 = rec;
        } else {
            rec2 = new Record();
            for (Field fld : rec.getFields()) {
                if (tags.contains(fld.getId())) {
                    rec.addField(fld);
                }
            }
        }
        final String strJson = rec2.toJSON3(0, convTable);
        
        return new JSONObject(new JSONTokener(strJson));
    }
    
    @Override
    protected final JSONObject getNext() {        
        JSONObject obj;
        
        try {            
            final int last = Math.min(to,
                           Math.max(0, mst.getControlRecord().getNxtmfn() - 1));
            
            if (current <= last) {
                final Record rec = mst.getRecord(current++);
                
                if (rec.isActive()) {
                    obj = getRecJson(rec, tags);
                } else {
                    obj = getNext();
                }
            } else {
                obj = null;
                mst.close();
            }                       
        } catch (BrumaException ex) {
            Logger.getLogger(this.getClass().getName()).severe(ex.getMessage());
            obj = getNext();
        } 
        
        return obj;
    }  
    
    private Map<Integer, String> getConvTable(final String in) 
                                                            throws IOException {
        assert in != null;
        
        final Map<Integer, String> map = new HashMap<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(in))) {
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                final String lineT = line.trim();
                if (!lineT.isEmpty()) {
                    final String[] split = lineT.split(" *= *", 2);
                    map.put(Integer.valueOf(split[0]), split[1]);
                }
            }
        }
        
        return map;
    }
}
