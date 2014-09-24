
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
import java.util.Iterator;
import java.util.List;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;

/**
 *
 * @author Heitor Barbieri
 * date: 20140916
 */
public class TweetLoader {
    public static final int MAX_PAGE_SIZE = 100;
    
    private final Twitter twitter;
    private final String userId;    
    private final int total;
    private final Date lowerDate;
    private final boolean useRetweets;
    
    private Query query;
    private Iterator<Status> tweetIterator;
    private Iterator<Status> retweetIterator;
    private int pageNum;    
    private int curTweet;
    private long max_id;

    public TweetLoader(final Twitter twitter,
                       final String userId,
                       final Query query,
                       final int total,
                       final Date lowerDate,
                       final boolean useRetweets) {
        if (twitter == null) {
            throw new NullPointerException("twitter");
        }
        if ((userId == null) && (query == null)) {
            throw new NullPointerException("userId AND query");	
        }
        if ((total <= 0) && (lowerDate == null)) {
            throw new IllegalArgumentException("total AND lowerDate");
        }
        this.twitter = twitter;
        this.userId = userId;
        this.query = query;
        this.total = total;
        this.lowerDate = lowerDate;
        this.useRetweets = useRetweets;

        tweetIterator = null;
        retweetIterator = null;
        pageNum = 0;
        curTweet = 0;
        max_id = Long.MAX_VALUE;
        
        if (query != null) {
            query.setCount(MAX_PAGE_SIZE);
        }
    }

    private void loadUserIdTweets() throws TwitterException {
        final Paging paging = new Paging(++pageNum, MAX_PAGE_SIZE);
        final ResponseList<Status> respLst =
                                    twitter.getUserTimeline(userId, paging);
        if ((lowerDate == null) || 
             respLst.get(respLst.size() - 1).getCreatedAt()
                                                        .after(lowerDate)) {
            tweetIterator = respLst.iterator();
        } else {
            tweetIterator = null;
        }
        retweetIterator = null;
    }
            
    private void loadQueryTweets() throws TwitterException {        
        if (query == null) {
            tweetIterator = null;
        } else {
            query.setMaxId(max_id);
            final QueryResult result = twitter.search(query); 
            if (result == null) {
                tweetIterator = null;
            } else {
                final List<Status> respLst = result.getTweets();
                if ((lowerDate == null) || 
                     respLst.get(respLst.size() - 1).getCreatedAt()
                                                            .after(lowerDate)) {
                    tweetIterator = respLst.iterator();
                } else {
                    tweetIterator = null;
                }
            }
        }
        retweetIterator = null;
    }

    private void getNextIterator() throws TwitterException {
        if (userId == null) {
            loadQueryTweets();            
        } else {
            loadUserIdTweets();
        }
    }

    public Status getNextStatus() throws TwitterException {
        final Status status;

        if ((retweetIterator != null) && (retweetIterator.hasNext())) {
            status = retweetIterator.next();
        }  else {
            if (tweetIterator == null) {
                getNextIterator();
            }
            if (tweetIterator == null) {
                status = null;
            } else if (++curTweet > total) { 
                status = null;
            } else if (tweetIterator.hasNext()) {
                status = tweetIterator.next();
                if (useRetweets) {
                    final ResponseList<Status> retweets = 
                                            twitter.getRetweets(status.getId());
                    if (retweets != null) {
                        retweetIterator= retweets.iterator();
                    }
                }        
            } else {
                tweetIterator = null;
                retweetIterator = null;
                curTweet--;
                status = getNextStatus();
            }
        }        
        if (status != null) {
            final long id = status.getId();
            if (id < max_id) {
                max_id = id - 1;
            }
        }
        return status;
    }
}