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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140808
 */
public class Json2Couch implements FromJson {
    public static final int DEFAULT_COUCH_PORT = 5984;
    
    private static final int MAX_BUFFER_SIZE = 4194304; // 4 mbytes
    private static final int MAX_BULK_DOCS = Integer.MAX_VALUE; //1000;

    private final URL url;
    private final char[] buffer;
    private final StringBuilder builder;
    private final Matcher matcher;
    private final String baseUrl;

    public Json2Couch(final String couchHost,
                      final String couchDbName,
                      final boolean append) throws MalformedURLException, 
                                                   IOException {                 
        this (couchHost, Integer.toString(DEFAULT_COUCH_PORT), null, null,
                                                           couchDbName, append);
    }
    
    public Json2Couch(final String couchHost,
                      final String couchPort,
                      final String couchUser,
                      final String couchPswd,
                      final String couchDbName,
                      final boolean append) throws MalformedURLException, 
                                                   IOException {                 
        assert couchHost != null;
        assert couchPort != null;
        assert couchDbName != null;

        final String userPswd = ((couchUser == null) || (couchPswd == null)) 
                                      ? "" : couchUser + ":" + couchPswd + "@";
        baseUrl = "http://" + userPswd + couchHost + ":" + couchPort + "/"; 
        url = new URL(baseUrl + couchDbName + "/_bulk_docs");
        buffer = new char[1024];
        builder = new StringBuilder();
        matcher = Pattern.compile("\"id\"\\:\"\\d+\",\"error\"\\:" +
                "\"[^\"]+\",\"reason\"\\:\"[^\"]+\"").matcher("");
        
        openDatabase(couchDbName, append);
    }

    private void openDatabase(final String couchDbName,
                              final boolean append) throws IOException {
        assert couchDbName != null;
        
        final URL dbasesUrl = new URL(baseUrl + "_all_dbs");
        final URL ourl = new URL(baseUrl + couchDbName);
        final HttpURLConnection hconn = 
                                  (HttpURLConnection)dbasesUrl.openConnection();                        
        hconn.setRequestMethod("GET");            
        hconn.connect();
        final BufferedReader reader = new BufferedReader(
                             new InputStreamReader(hconn.getInputStream()));
        boolean exists = false;
        
        while (!exists) {
            final String line = reader.readLine();
            if (line == null) {
                break;
            }
            exists = line.contains("\"" + couchDbName + "\"");
        }
        reader.close();
                
        if (exists) {
            if (!append) { // Reset database if required
                final HttpURLConnection conn2 = 
                                       (HttpURLConnection)ourl.openConnection();                        
                conn2.setRequestMethod("DELETE");            
                conn2.connect();
                new InputStreamReader(conn2.getInputStream()).close();
                final HttpURLConnection conn3 = 
                                       (HttpURLConnection)ourl.openConnection();                       
                conn3.setRequestMethod("PUT");
                conn3.connect();   
                new InputStreamReader(conn3.getInputStream()).close();
            }
        } else {  // Create database        
            final HttpURLConnection conn3 = 
                                       (HttpURLConnection)ourl.openConnection();                       
            conn3.setRequestMethod("PUT");
            conn3.connect();   
            new InputStreamReader(conn3.getInputStream()).close();
        }
    }
    
    private List<String> getBadDocuments(final Reader reader) 
                                                            throws IOException {
        assert reader != null;
        
        final List<String> msgs = new ArrayList<>();
            
        builder.setLength(0);
        while (true) {        
            final int read = reader.read(buffer);
            if (read == -1) {
                break;
            }
            builder.append(buffer, 0, read);
        }
        matcher.reset(builder);
        while (matcher.find()) {
            msgs.add(matcher.group(0));
        }
        return msgs;
    }
    
    private void sendDocuments(final String docs) {
        try {
            assert docs != null;
            
            final URLConnection connection = url.openConnection();
            connection.setRequestProperty("CONTENT-TYPE", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.connect();
            
            try (OutputStreamWriter out = new OutputStreamWriter(
                    connection.getOutputStream())
                    //final String udocs = URLEncoder.encode(docs, "UTF-8");
                    ) {
                out.write(docs);
            }
            
            try (InputStreamReader reader = new InputStreamReader(
                    connection.getInputStream())) {
                final List<String> msgs = getBadDocuments(reader);
                
                for (String msg: msgs) {
                    System.err.println("write error: " + msg);
                }
            }
        }   catch (IOException ex) {
            Logger.getLogger(Json2Couch.class.getName())
                                                   .log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     *
     * @param e2j
     * @param tell
     */
    @Override
    public void exportDocuments(final Iterable<JSONObject> e2j,
                                final int tell) {
        if (e2j == null) {
            throw new NullPointerException("e2j");
        }
        if (tell <= 0) {
            throw new IllegalArgumentException("tell <= 0");
        }
        int tot = 0;
        int bulkNum = 0;
        boolean first = true;
        
        builder.setLength(0);
                
        for (JSONObject obj : e2j) {       
            if (bulkNum == 0) {
                builder.append("{\n \"docs\": [\n");
            }
                            
            if (++tot % tell == 0) {
                System.out.println("+++" + tot);
            }
            bulkNum++;
            if (first) {
                first = false;
            } else {
                builder.append(",");
            }
            builder.append(obj.toString());
            builder.append("\n");
            if ((builder.length() >= MAX_BUFFER_SIZE) || 
                (bulkNum >= MAX_BULK_DOCS)) {
                first = true;
                bulkNum = 0;
                builder.append(" ]\n}\n");
                sendDocuments(builder.toString());
                builder.setLength(0);
            }
        }
        if (builder.length() > 0) {
            builder.append(" ]\n}\n");
            sendDocuments(builder.toString());
        }
    }        
}
