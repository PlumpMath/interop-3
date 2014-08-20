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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class Json2Lucene implements FromJson {
    final Directory directory;
    final IndexWriter iwriter;
    final Field.Store store;
    
    public Json2Lucene(final String luceneDir,
                       final boolean store,
                       final boolean append) throws IOException {
        if (luceneDir == null) {
            throw new NullPointerException("luceneDir");
        }
        final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);                        
        final IndexWriterConfig config = new IndexWriterConfig(
                                                  Version.LUCENE_4_9, analyzer);
        
        config.setOpenMode(append ? IndexWriterConfig.OpenMode.APPEND
                                  : IndexWriterConfig.OpenMode.CREATE);
        directory = new SimpleFSDirectory(new File(luceneDir));
        iwriter = new IndexWriter(directory, config);
        this.store = store ? Field.Store.YES : Field.Store.NO;
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
            int tot = 0;
            
            for (JSONObject obj : it) {
                if (++tot % tell == 0) {
                    System.out.println("+++" + tot);
                }
                iwriter.addDocument(convertToDoc(obj));
            }
            iwriter.close();
            directory.close();
        } catch (IOException ex) {
            Logger.getLogger(Json2Lucene.class.getName())
                                                   .log(Level.SEVERE, null, ex);
        }
    }
    
    private Document convertToDoc(JSONObject jobj) {
        assert jobj != null;
        
        final Document doc = new Document();
        final Iterator<String> it = jobj.keys();
        
        while (it.hasNext()) {
            final String key = it.next();
            final Object obj = jobj.get(key);
            
            if (obj != null) {
                final ArrayList<Field> fields = new ArrayList<>();
                for (Field field : getFields(key, obj, fields)) {
                    doc.add(field);
                }
            }
        }        
        return doc;
    }
    
    private ArrayList<Field> getFields(final String key,
                                       final Object obj,
                                       final ArrayList<Field> fields) {
        assert key != null;
        assert obj != null;
        assert fields != null;
        
        if (obj instanceof JSONArray) {
            final JSONArray jarray = (JSONArray)obj;
            final int len = jarray.length();
            
            for (int idx = 0; idx < len; idx++) {
                getFields(key, jarray.get(idx), fields);
            }
        } else if (obj instanceof JSONObject) {
            final JSONObject jobj = (JSONObject)obj;
            final Iterator<String> it = jobj.keys();
            final StringBuilder builder = new StringBuilder();
            while (it.hasNext()) {
                final String k1 = it.next();
                builder.append("<");
                builder.append(k1);
                builder.append(">");
                builder.append(jobj.get(k1).toString());
            }
            fields.add(new TextField(key, builder.toString(), store));
        } else if (obj instanceof String) {
            fields.add(new TextField(key, (String)obj, store));
        } else if (obj instanceof Integer) {
            fields.add(new IntField(key, (Integer)obj, store));
        } else if (obj instanceof Long) {
            fields.add(new LongField(key, (Long)obj, store));
        } else if (obj instanceof Double) {
            fields.add(new DoubleField(key, (Double)obj, store));
        } else if (obj instanceof Float) {
            fields.add(new FloatField(key, (Float)obj, store));    
        } else {
            throw new IllegalArgumentException("unexpected object type: " 
                                                          + obj.toString());
        }          
        return fields;
    }    
}
