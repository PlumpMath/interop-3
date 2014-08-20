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

import java.io.File;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date: 20140814
 */
public class Lucene2Json extends ToJson {
    private final String luceneDir;
    private final String defQueryFld;
    
    private Directory directory;
    private DirectoryReader ireader;
    private IndexSearcher isearcher ;
    private ScoreDoc[] hits;
    private int curIndex;
    private int lastIndex;
    
    public Lucene2Json(final String luceneDir,
                       final String defQueryFld) {
        if (luceneDir == null) {
            throw new NullPointerException("luceneDir");
        }
        if (defQueryFld == null) {
            throw new NullPointerException("defQueryFld");
        }
        this.luceneDir = luceneDir;
        this.defQueryFld = defQueryFld;
        directory = null;
        ireader = null;
        isearcher = null;
        hits = null;
        curIndex = 0;
        lastIndex = 0;
    }
    
    public void setQuery(final String squery,
                         final int from,
                         final int to) throws Exception {
        if (squery == null) {
            throw new NullPointerException("squery");
        }
        if (from < 1) {
            throw new IllegalArgumentException("from[" + from + "] < 1");
        }
        if (to < from) {
            throw new IllegalArgumentException("to[" + to + "] < from[" + from
                                                                         + "]");
        }
        this.to = to;
        this.from = from;
     
        final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        
        directory = new SimpleFSDirectory(new File(luceneDir));                
        ireader = DirectoryReader.open(directory);
        isearcher = new IndexSearcher(ireader);
        
        final QueryParser parser = new QueryParser(Version.LUCENE_4_9, 
                                                         defQueryFld, analyzer);
        final Query query = parser.parse(squery);
        
        hits = isearcher.search(query, to).scoreDocs;
        curIndex = from - 1;
        lastIndex = hits.length - 1;
    }
    
    @Override
    protected JSONObject getNext() throws Exception {
        final JSONObject obj;
        
        if (curIndex <= lastIndex) {
            final Document hitDoc = isearcher.doc(hits[curIndex++].doc);
            obj = convertToJSONObject(hitDoc);
        } else {
            obj = null;
            ireader.close();
            directory.close();
        }
        
        return obj;
    }   
    
    private JSONObject convertToJSONObject(final Document hitDoc) {
        assert hitDoc != null;
        
        final JSONObject obj = new JSONObject();
        
        for (IndexableField field : hitDoc.getFields()) {            
            final String key = field.name();
            final Number number = field.numericValue();
            
            if (number == null) {
                obj.append(key, field.stringValue());
            } else {
                obj.append(key, number);
            }
        }
        
        return obj;
    }
}
