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

import au.com.bytecode.opencsv.CSVWriter;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140821
 */
public class Json2Csv implements FromJson {
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final char DEFAULT_DELIMITER = ',';
    
    private final CSVWriter csvwriter;
    private final Writer writer;
    private boolean hasFooter;
    
    public Json2Csv(final String csvFile,
                    final boolean hasFooter) throws IOException {
        this (csvFile, DEFAULT_ENCODING, DEFAULT_DELIMITER, hasFooter);
    }
    
    public Json2Csv(final String csvFile,
                    final String csvEncoding,
                    final char csvDelimiter,
                    final boolean hasFooter) throws IOException {
        if (csvFile == null) {
            throw new NullPointerException("csvFile");
        }
        if (csvEncoding == null) {
            throw new NullPointerException("csvEncoding");
        }
        writer = new BufferedWriter(new OutputStreamWriter(
                                   new FileOutputStream(csvFile), csvEncoding));
        csvwriter = new CSVWriter(writer, csvDelimiter);
        this.hasFooter = hasFooter;
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
        final Map<String,Integer> footer = new TreeMap<>();
        final ArrayList<String> line = new ArrayList<>();
        final String[] stra = new String[0];
        int tot = 0;
        
        for (JSONObject obj : it) {           
            if (++tot % tell == 0) {
                System.out.println("+++" + tot);
            } 
            line.clear();
            final Iterator<String> kit = obj.keys();
                
            while (kit.hasNext()) {
                final String k1 = kit.next();
                Integer idx = footer.get(k1);
                if (idx == null) {
                    idx = footer.size();
                    footer.put(k1, idx);
                }
                AddElement(line, idx, obj.get(k1).toString());
            }
            csvwriter.writeNext(line.toArray(stra));
        }
        if (hasFooter) { // Header at the end of the file
            csvwriter.writeNext(footer.keySet().toArray(stra));
        }
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Json2Csv.class.getName()).log(Level.SEVERE, null
                                                                          , ex);
        }
    }
    
    private void AddElement(final ArrayList<String> buffer,
                            final int index,
                            final String value) {
        assert buffer != null;
        assert index >= 0;
        assert value != null;
        
        final int size = buffer.size();
        
        if (index < size) {
            buffer.add(index, value);
        } else {
            for (int idx = size; idx < index; idx++) {
                buffer.add(null);
            }
            buffer.add(value);
        }
    }
}
