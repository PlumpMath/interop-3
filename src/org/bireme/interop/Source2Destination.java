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

package org.bireme.interop;

import org.bireme.interop.fromJson.FromJson;
import org.bireme.interop.toJson.ToJson;

/**
 *
 * @author Heitor Barbieri
 * date: 20140808
 */
public class Source2Destination {
    private final ToJson tj;
    private final FromJson fj;
    
    public Source2Destination(final ToJson tj,
                              final FromJson fj) {
        if (tj == null) {
            throw new NullPointerException("tj");
        }        
        if (fj == null) {
            throw new NullPointerException("fj");
        }
        this.tj = tj;
        this.fj = fj;
    }
    
    public void export(final int tell) {
        if (tell <= 0) {
            throw new IllegalArgumentException("tell <= 0");
        }
        fj.exportDocuments(tj, tell);
    }    
}
