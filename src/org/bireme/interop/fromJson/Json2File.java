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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140918
 */
public class Json2File implements FromJson {
    public static final String DEFAULT_ENCODING = "utf-8";
    
    final File outFile;
    final String encoding;
    final boolean oneFileOnly;
    int current;
    
    public Json2File(final String outFile,
                     final String encoding,
                     final boolean oneFileOnly) {
        if (outFile == null) {
            throw new NullPointerException("outFile");
        }
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
        this.outFile = new File(outFile);
        if (this.outFile.isDirectory()) {
            throw new IllegalArgumentException("outFile[" + outFile 
                                                           + " is a directory");
        }
        this.encoding = encoding;
        this.oneFileOnly = oneFileOnly;
        current = 0;
    }
    
    private File renameFile(final File inFile,
                            final int index) throws IOException {
        assert inFile != null;
        assert index >= 0;
        
        final String fnameIn = inFile.getCanonicalPath();
        final int pos = fnameIn.lastIndexOf('.');
        final String fnameOut = (pos > 0) 
              ? fnameIn.substring(0, pos) + "_" + index + fnameIn.substring(pos)
              : fnameIn + "_" + index;
        return new File(fnameOut);
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
        BufferedWriter writer = null;
        int tot = 0;
        
        try {
            if (oneFileOnly) {
                writer = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(outFile), encoding));
                writer.append("[\n");
            }
            for (JSONObject obj : it) {
                if (++tot % tell == 0) {
                    System.out.println("+++" + tot);
                } 
                if (oneFileOnly) {
                    if (tot > 1) {
                        writer.append(",");
                    }
                } else {
                    writer = new BufferedWriter(new OutputStreamWriter(
                           new FileOutputStream(renameFile(outFile, ++current)), 
                                                encoding));                            
                }
                writer.append(obj.toString(2));                
                writer.append("\n");
                if (! oneFileOnly) {
                    writer.close();
                }
            }
            if (oneFileOnly) {
                writer.append("]");                
                writer.close();                
            }            
        } catch (IOException ex) {
            Logger.getLogger(Json2File.class.getName())
                                                   .log(Level.SEVERE, null, ex);
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ex2) {
                    Logger.getLogger(Json2File.class.getName())
                                                  .log(Level.SEVERE, null, ex2);
                }    
            }
        }                    
    }    
}
