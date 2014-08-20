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

import org.bireme.interop.fromJson.Json2Mongo;
import org.bireme.interop.toJson.Mongo2Json;

/**
 *
 * @author Heitor Barbieri
 * date: 20140815
 */
public class Mongo2Mongo extends Source2Destination {

    public Mongo2Mongo(final Mongo2Json m2j, 
                       final Json2Mongo j2m) {
        super(m2j, j2m);
    }
    
    private static void usage() {
        System.err.println("usage: Mongo2Mongo <mongohost1> <mongodb1> "
                   + "<mongocol1> <mongoquery1> <mongohost2> <mongodb2> "
                   + "<mongocol2> OPTIONS");
        System.err.println();
        System.err.println("       <mongohost1> - Source MongoDB server url.");
        System.err.println("       <mongodb1> - Source MongoDB database.");
        System.err.println("       <mongocol1> - Source MongoDB colection name.");
        System.err.println("       <mongoquery1> - Search to retrieve the documents"
                                     + "to be exported from MongoDB database.");
        System.err.println("       <mongohost2> - Destination MongoDB server url.");
        System.err.println("       <mongodb2> - Destination MongoDB database.");
        System.err.println("       <mongocol2> - Destination MongoDB colection name.");
        System.err.println();
        System.err.println("OPTIONS:");
        System.err.println();
        System.err.println("       --mongoport1=<port>");
        System.err.println("           Source MongoDb server port.");
        System.err.println("       --mongouser1=<user>");
        System.err.println("           Source MongoDB server user.");
        System.err.println("       --mongopsw1=<password>");  
        System.err.println("           Source MongoDB server password.");
        System.err.println("       --mongofrom1=<from>");         
        System.err.println("           Initial sequential response hit number.");
        System.err.println("       --mongoto1=<to>");    
        System.err.println("           Last sequential response hit number.");
        System.err.println("       --mongoport2=<port>");        
        System.err.println("           Destination MongoDb server port.");
        System.err.println("       --mongouser2=<user>");
        System.err.println("           Destination MongoDB server user.");
        System.err.println("       --mongopsw2=<password>");      
        System.err.println("           Destination MongoDB server password.");
        System.err.println("       --tell=<num>");
        System.err.println("           Outputs log message each <num> exported documents.");
        System.err.println("       --append");        
        System.err.println("           Appends the documents into the destination MongoDB collection.");
        
        System.exit(1);
    }
    
    public static void main(final String[] args) throws Exception  {        
        final int len = args.length;
        if (len < 7) {
            usage();
        }
        
        final String mongoHost1 = args[0];
        final String mongoDbName1 = args[1];
        final String mongoColName1 = args[2];
        final String mongoQuery1 = args[3];
        final String mongoHost2 = args[4];
        final String mongoDbName2 = args[5];
        final String mongoColName2 = args[6];
        
        String mongoPort1 = Integer.toString(Mongo2Json.DEFAULT_MONGO_PORT);
        String mongoPort2 = Integer.toString(Mongo2Json.DEFAULT_MONGO_PORT);
        String mongoUser1 = null;
        String mongoUser2 = null;
        String mongoPswd1 = null;
        String mongoPswd2 = null;
        int mongoFrom1 = 1;
        int mongoTo1 = Integer.MAX_VALUE;
                                      
        boolean append = false;        
        int tell = Integer.MAX_VALUE;
        
        for (int idx = 7; idx < len; idx++) {
            if (args[idx].startsWith("--mongoport1=")) {
                mongoPort1 = args[idx].substring(13);
            } else if (args[idx].startsWith("--mongouser1=")) {
                mongoUser1 = args[idx].substring(13);
            } else if (args[idx].startsWith("--mongopsw1=")) {
                mongoPswd1 = args[idx].substring(12); 
            } else if (args[idx].startsWith("--mongofrom1=")) {
                mongoFrom1 = Integer.parseInt(args[idx].substring(13)); 
            } else if (args[idx].startsWith("--mongoto1=")) {
                mongoTo1 = Integer.parseInt(args[idx].substring(11)); 
            } else if (args[idx].startsWith("--mongoport2=")) {
                mongoPort2 = args[idx].substring(13);
            } else if (args[idx].startsWith("--mongouser2=")) {
                mongoUser2 = args[idx].substring(13);
            } else if (args[idx].startsWith("--mongopsw2=")) {
                mongoPswd2 = args[idx].substring(12); 
            } else if (args[idx].startsWith("--tell=")) {
                tell = Integer.parseInt(args[idx].substring(7));
            } else if (args[idx].equals("--append")) {
                append = true;
            } else { 
                usage();
            }
        }
        
        final Mongo2Json m2j = new Mongo2Json(mongoHost1,
                                              mongoPort1,
                                              mongoUser1,
                                              mongoPswd1,
                                              mongoDbName1,
                                              mongoColName1);
        m2j.setQuery(mongoQuery1, mongoFrom1, mongoTo1);
        
        final Json2Mongo j2m = new Json2Mongo(mongoHost2,
                                              mongoPort2,
                                              mongoUser2,
                                              mongoPswd2,
                                              mongoDbName2,
                                              mongoColName2,
                                              append);
        
        new Mongo2Mongo(m2j, j2m).export(tell);
    }
}
