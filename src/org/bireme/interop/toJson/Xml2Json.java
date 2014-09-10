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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.json.JSONObject;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *
 * @author Heitor Barbieri 
 * date 20140907
 */
public class Xml2Json extends ToJson {

    public static final String DEF_EXTENSION = "[Xx][Mm][Ll]";

    private final Iterator<File> files;
    private final DocumentBuilder builder;
    private final Map<String, XPathExpression> xpath;

    public Xml2Json(final String filePath,
                    final String fileNameRegExp,
                    final String xpathTable) throws IOException, 
                                                  XPathExpressionException,
                                                  ParserConfigurationException {
        this(filePath, fileNameRegExp, DEF_EXTENSION, true, xpathTable, null);
    }

    public Xml2Json(final String filePath,
                    final String fileNameRegExp,
                    final String fileExtRegExp,
                    final boolean recursive,
                    final String xpathTable,
                    final String containsRegExp) throws IOException, 
                                                  XPathExpressionException,
                                                  ParserConfigurationException {
        if (filePath == null) {
            throw new NullPointerException("filePath");
        }
        if (fileNameRegExp == null) {
            throw new NullPointerException("fileNameRegExp");
        }
        if (xpathTable == null) {
            throw new NullPointerException("xpathTable");
        }
        files = getFiles(filePath, fileNameRegExp, fileExtRegExp, recursive)
                .iterator();
        xpath = getMap(xpathTable);
        next = getNext();        
        builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }

    private Map<String, XPathExpression> getMap(final String xpathTable)
                                               throws IOException, 
                                                      XPathExpressionException {
        assert xpathTable != null;

        final XPath xPath =  XPathFactory.newInstance().newXPath();
        final Map<String, XPathExpression> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(
                                                  new FileReader(xpathTable))) {
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                
                final String[] split = line.trim().split(" *= *", 2);
                if (split.length != 2) {
                    throw new IOException("invalid line format: " + line);
                }
                map.put(split[0], (XPathExpression) xPath.compile(split[1]));
            }
        }

        return map;
    }

    private void getFiles(final File filePath,
                          final Matcher fileNameRegExp,
                          final Matcher fileExtRegExp,
                          final boolean recursive,
                          final List<File> out) throws IOException {
        assert filePath != null;
        assert fileNameRegExp != null;
        assert out != null;
        
        if (filePath.exists()) {
            if (filePath.isFile()) { // regular file
                final String path = filePath.getName();
                final int dot = path.lastIndexOf('.');
                final String fname = (dot == -1)? path : path.substring(0, dot);  
                final String ext = (dot == -1) ? null : path.substring(dot + 1);
                
                if (fileNameRegExp.reset(fname).matches()) {
                    if (fileExtRegExp == null) {
                        if (ext == null) {
                            out.add(filePath);
                        }
                    } else if ((ext != null) && 
                               (fileExtRegExp.reset(ext).matches())) {
                        out.add(filePath);
                    }
                }                                
            } else { // directory
                if (recursive) {
                    final File[] files = filePath.listFiles();
                    
                    if (files != null) {
                        for (File file : filePath.listFiles()) {
                            getFiles(file, fileNameRegExp, fileExtRegExp, recursive, 
                                                                               out);
                        }
                    }
                }
            }
        }
    }
    
    private List<File> getFiles(final String filePath,
                                final String fileNameRegExp,
                                final String fileExtRegExp,
                                final boolean recursive) throws IOException {
        assert filePath != null;
        assert fileNameRegExp != null;
        
        final List<File> lst = new ArrayList<>();
        
        getFiles(new File(filePath), 
                 Pattern.compile(fileNameRegExp).matcher(""),
                 (fileExtRegExp == null) ? null : 
                                     Pattern.compile(fileExtRegExp).matcher(""),
                 recursive,
                 lst);
        
        return lst;
    }
    
    private List<File> getFiles(final String filePath,
                                final String fileNameRegExp,
                                final String fileExtRegExp,
                                final boolean recursive,
                                final String containsRegExp) throws IOException {
        final List<File> lst1 = new ArrayList<>();
        final List<File> lst2 = getFiles(filePath, fileNameRegExp, 
                                         fileExtRegExp, recursive);
        final Matcher mat = Pattern.compile(containsRegExp).matcher("");
        
        for (File file : lst2) {
            if (contains(file, mat)) {
                lst1.add(file);
            }
        }
        return lst1;
    }

    private boolean contains(final File in,
                             final Matcher mat) throws IOException {
        assert in != null;
        assert mat != null;
        
        boolean ret = false;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(in))) {
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                
                mat.reset(line);
                if (mat.find()) {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    private JSONObject getJson(final File in) throws IOException {
        assert in != null;
        
        final JSONObject obj = new JSONObject();
        try (FileReader reader = new FileReader(in)) {
            final InputSource is = new InputSource(reader);
            
            for (Map.Entry<String,XPathExpression> entry : xpath.entrySet()) {
                final String key = entry.getKey();
                final NodeList nodeList = (NodeList) entry.getValue()
                        .evaluate(is, XPathConstants.NODESET);
                final int len = nodeList.getLength();
                
                for (int idx = 0; idx < len; idx++) {
                    final Node node = nodeList.item(idx);
                    obj.append(key, node.getNodeValue());
                }
            }
        } catch (XPathExpressionException ex) {
            throw new IOException(ex);
        }
        
        return obj;
    }

    @Override
    protected final JSONObject getNext() {
        JSONObject obj;

        if (files.hasNext()) {
            try {
                obj = getJson(files.next());
            } catch (IOException ioe) {
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
