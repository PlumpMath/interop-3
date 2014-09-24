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

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Query;
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
    private final TweetLoader loader;

    public Twitter2Json(final String userId,
                        final int total,
                        final Date lowerDate,
                        final boolean useRetweets) throws TwitterException {
        this(userId, null, total, lowerDate, useRetweets);
    }

    public Twitter2Json(final Query query,
                        final int total,
                        final Date lowerDate,
                        final boolean useRetweets) throws TwitterException {
        this(null, query, total, lowerDate, useRetweets);
    }

    public Twitter2Json(final String userId,
                         final Query query,
                         final int total,
                         final Date lowerDate,
                         final boolean useRetweets) throws TwitterException {
        assert ((userId != null) || (query != null));
        assert ((total >= 1) || (lowerDate != null));
        
        final ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("LmvfpL4pJE7PJr0CgJUuQKZOy")
          .setOAuthConsumerSecret("UP1VIf4K3jGDvOYYXh0zshHnuj8HSUMDTWlJEEhGMWIJHKGXSJ")
          .setOAuthAccessToken("2788081964-9C7j4ZBOsdIy9tmtMkong54QimvjkwopXdLqecm")
          .setOAuthAccessTokenSecret("swwqLPLWppweF3KRKe2zpU9qPNFDbsZm9OqQ5scX9vVCp");

        final Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        
        this.loader = new TweetLoader(twitter, userId, query, total, lowerDate, 
                                                                   useRetweets);
        next = getNext();
    }

    @Override
    protected final JSONObject getNext() {
        Status status;
        try {
            status = loader.getNextStatus();
        } catch (TwitterException ex) {
            Logger.getLogger(Twitter2Json.class.getName()).log(Level.SEVERE, null, ex);
            status = null;
        }
        
        return (status == null) ? null : getDocument(status);
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