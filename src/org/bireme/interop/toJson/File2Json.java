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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri 
 * date 20140908
 */
    public class File2Json extends ToJson {
    public static final String DEFAULT_ENCODING = "UTF-8";
    
    private final Iterator<File> files;
    private final String encod;

    public File2Json(final String filePath,
                     final String fileNameRegExp) throws IOException {
        this(filePath, fileNameRegExp, DEFAULT_ENCODING, true);
    }

    public File2Json(final String filePath,
                     final String fileNameRegExp,
                     final String encoding,
                     final boolean recursive) throws IOException {
        if (filePath == null) {
            throw new NullPointerException("filePath");
        }
        if (fileNameRegExp == null) {
            throw new NullPointerException("fileNameRegExp");
        }
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
        encod = encoding;
        files = getFiles(filePath, fileNameRegExp, recursive)
                .iterator();
        next = getNext();                
    }

    private void getFiles(final File filePath,
                          final Matcher fileNameMat,
                          final boolean recursive,
                          final List<File> out) throws IOException {
        assert filePath != null;
        assert fileNameMat != null;
        assert out != null;
        
        if (filePath.exists()) {
            if (filePath.isFile()) { // regular file
                final String path = filePath.getName();

                if (fileNameMat.reset(path).matches()) {
                out.add(filePath);
                }                                
            } else { // directory
                if (recursive) {
                     final File[] xfiles = filePath.listFiles();
                     if (xfiles != null) {                         
                        for (File file : filePath.listFiles()) {
                            getFiles(file, fileNameMat, recursive, out);
                        }
                    }
                }
            }
        }
    }
    
    private List<File> getFiles(final String filePath,
                                final String fileNameRegExp,
                                final boolean recursive) throws IOException {
        assert filePath != null;
        assert fileNameRegExp != null;
        
        final List<File> lst = new ArrayList<>();
        
        getFiles(new File(filePath), 
                 Pattern.compile(fileNameRegExp).matcher(""),                 
                 recursive,
                 lst);
        
        return lst;
    }
    

    private JSONObject getJson(final File in) throws IOException {
        assert in != null;
        
        final JSONObject jobj;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                                             new FileInputStream(in), encod));
        final StringBuilder builder = new StringBuilder();
        
        while (true) {
            final String line = reader.readLine();
            if (line == null) {
                break;
            }
            builder.append(line);
        }        
        reader.close();
        
        try {
            jobj = new JSONObject(builder.toString());
        } catch(JSONException jex) {
            throw new IOException("[" + in.getCanonicalPath() + "]: " 
                                                           + jex.getMessage());
        }
        return jobj;
    }

    @Override
    protected final JSONObject getNext() {
        JSONObject obj;

        if (files.hasNext()) {
            try {
                obj = getJson(files.next());
            } catch(IOException ioe) {
                Logger.getLogger(this.getClass().getName())
                                                      .severe(ioe.getMessage());
                obj = getNext();
            }
        } else {
            obj = null;
        }

        return obj;
    }
}
