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
import java.util.logging.Logger;
import org.json.JSONObject;
import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.Place;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author Heitor Barbieri
 * date: 20140904
 */
public class Twitter2Json extends ToJson {
    // See https://dev.twitter.com/docs/rate-limiting/1.1
    public static final int MAX_RATE_LIMIT = 150;     

    private final boolean useRetweets;
    private final Twitter twitter;
    private final Iterator<Status> tweetIterator;
    
    private Iterator<Status> retweetIterator;
    private int total;
    
    public Twitter2Json(final String userId,
                        final int from,
                        final int to,
                        final boolean useRetweets) throws TwitterException {
        if (userId == null) {
            throw new NullPointerException("userId");
        }        
        if (from < 1) {
            throw new IllegalArgumentException("from[" + from + "] < 1");
        }        
        if (to < from) {
            throw new IllegalArgumentException("to[" + to + "] <= from[" + from 
                                                                         + "]");
        }
        if ((to-from) >= MAX_RATE_LIMIT) {
            throw new IllegalArgumentException("to-from >= " + MAX_RATE_LIMIT);
        }
        this.from = from;
        this.to = to;
        this.useRetweets = useRetweets;
        this.total = from;
        
        final ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("LmvfpL4pJE7PJr0CgJUuQKZOy")
          .setOAuthConsumerSecret("UP1VIf4K3jGDvOYYXh0zshHnuj8HSUMDTWlJEEhGMWIJHKGXSJ")
          .setOAuthAccessToken("2788081964-9C7j4ZBOsdIy9tmtMkong54QimvjkwopXdLqecm")
          .setOAuthAccessTokenSecret("swwqLPLWppweF3KRKe2zpU9qPNFDbsZm9OqQ5scX9vVCp");
        
        final Paging paging = new Paging(from, to);                

        this.twitter = new TwitterFactory(cb.build()).getInstance();               
        final ResponseList<Status> respLst = 
                                        twitter.getUserTimeline(userId, paging);
        
        this.tweetIterator = respLst.iterator();
        this.retweetIterator = null;
        this.next = getNext();
    }
    
    @Override
    protected final JSONObject getNext() {
        JSONObject obj = null;
        boolean found = false;
        
        if (total++ <= to) {
            if (retweetIterator != null) {
                if (retweetIterator.hasNext()) {
                    obj = getDocument(retweetIterator.next());
                    found = true;
                } else {
                    retweetIterator = null;
                }
            }
            if (!found) {
                if (tweetIterator.hasNext()) {                    
                    final Status status = tweetIterator.next();
                    obj = getDocument(status);
                    if (useRetweets) {
                        try {
                            retweetIterator = twitter.getRetweets(status.getId())
                                                                    .iterator();
                        } catch(TwitterException tex) {
                            retweetIterator = null;
                            Logger.getLogger(this.getClass().getName())
                                                      .severe(tex.getMessage());
                        }
                    }
                } else {
                    obj = null;
                }
            }
        }
        
        return obj;
    }
    
    private JSONObject getDocument(final Status status) {
        assert status != null;
        
        final JSONObject obj = new JSONObject();
        final GeoLocation geo = status.getGeoLocation();
        final Place place = status.getPlace();
        final User user = status.getUser();
        
        obj.put("createdAt", status.getCreatedAt())
           .put("id", status.getId())
           .put("lang", status.getLang());
        if (geo != null) {
           obj.put("location_latitude", geo.getLatitude())
              .put("location_longitude", geo.getLongitude());           
        }
        if (place != null) {
           obj.put("place_country", place.getCountry())
              .put("place_fullName", place.getFullName())
              .put("place_id", place.getId())
              .put("place_name", place.getName())
              .put("place_type", place.getPlaceType())
              .put("place_streetAddress", place.getStreetAddress())
              .put("place_url", place.getURL());
        }
        obj.put("source", status.getSource())
           .put("text", status.getText());
        if (user != null) {
           obj.put("user_description", user.getDescription())
              .put("user_id", user.getId())
              .put("user_lang", user.getLang())
              .put("user_location", user.getLocation())
              .put("user_name", user.getName())
              .put("user_url", user.getURL());
        }
        obj.put("isTruncated", status.isTruncated())
           .put("isRetweet", status.isRetweet());        
        
        return obj;
    }    
}