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

import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140821
 */
public class Csv2Json extends ToJson {
    public static final char DEF_DELIMITER = ',';
    
    private final String csvFileName;
    private final Reader reader;
    private final CSVReader csvreader;
    private final String[] header;
    
    public Csv2Json(final String csvFile,
                    final boolean firstLineIsHeader) throws Exception {
        this(csvFile, "UTF-8", DEF_DELIMITER, firstLineIsHeader);
    }
    
    public Csv2Json(final String csvFile,
                    final String encoding,
                    final char delimiter,
                    final boolean firstLineIsHeader) throws Exception {
        if (csvFile == null) {
            throw new NullPointerException("csvFile");
        }
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
        csvFileName = csvFile;
        reader = new BufferedReader(new InputStreamReader(
                                       new FileInputStream(csvFile), encoding));
        csvreader = new CSVReader(reader, delimiter);
        header = getHeader(firstLineIsHeader);
        next = getNext();
    }

    private String[] getHeader(final boolean firstLineIsHeader) 
                                                            throws IOException {
        final String[] str;
        
        if (firstLineIsHeader) {
            final String[] nextLine = csvreader.readNext();
            if (nextLine == null) {
                str = null;
            } else {
                str = nextLine;
            }            
        } else {
            str = null;
        }
        
        return str;
    }
    
    @Override
    protected final JSONObject getNext() {        
        JSONObject jobj;

        try {
            final String[] nextLine = csvreader.readNext();
            if (nextLine == null) {
                reader.close();
                jobj = null;
            } else {
                final int size = nextLine.length;
                jobj = new JSONObject();
                if ((header != null) && (header.length != size)) {
                    final String msg = "[" + csvFileName + "]: csv line length";
                    Logger.getLogger(this.getClass().getName()).severe(msg);
                    jobj = getNext();
                } else {
                    for (int idx = 0; idx < size; idx++) {
                        final String key = (header == null) ? "c" + (idx + 1) 
                                                            : header[idx];
                        jobj.put(key, nextLine[idx]);
                    }
                }
            }
        } catch (IOException ioe) {
            final String msg = "[" + csvFileName + "]: " + ioe.getMessage();
            Logger.getLogger(this.getClass().getName()).severe(msg);
            jobj = null;
        }
        
        return jobj;
    }    
}
