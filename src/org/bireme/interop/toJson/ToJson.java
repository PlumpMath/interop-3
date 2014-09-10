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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 *
 * @author Heitor Barbieri
 * date 20140807
 */
public abstract class ToJson implements Iterable<JSONObject>, 
                                                          Iterator<JSONObject> {
    protected int from;
    protected int to;        
    protected JSONObject next;
    
    protected ToJson() {
        from = 1;
        to = Integer.MAX_VALUE;
        next = null;
    }
    
    protected abstract JSONObject getNext();
    
    @Override
    public Iterator<JSONObject> iterator() {
        return this;
    }
    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public JSONObject next() {
        if (! hasNext()) {
            throw new NoSuchElementException();
        }
        final JSONObject ret = next;
        try {
            next = getNext();
        } catch (Exception ex) {
            Logger.getLogger(this.getClass().getName())
                                                   .log(Level.SEVERE, null, ex);
        }
        
        return ret;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }    
}
