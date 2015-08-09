var path = require('path');
var crypto = require('crypto');
var Promise = require('bluebird');
var redis = Promise.promisifyAll(require('redis'));
var through2 = require('through2');
var utils = require('ympc-utils');

exports = module.exports = db;

var padTime = utils.padTime;
var removeNonScalars = utils.removeNonScalars;

function db (options) {
  options = options || {};

  if (!options.socket) {
    options.host = options.host || '127.0.0.1';
    options.port = parseInt(options.port, 10) || 6379;
  }

  var redisOptions = {};
  if (options.password) { redisOptions.auth_pass = options.password; }

  var client;

  if (options.socket) {
    client = redis.createClient(options.socket, redisOptions);
  } else {
    client = redis.createClient(options.port, options.host, redisOptions);
  }

  function makeShowKey (showId) {
    if (typeof showId === 'object') { showId = showId.id; }
    return 'show:' + showId;
  }

  function makeEpisodeKey (showId, episodeSien) {
    if (typeof episodeSien === 'object') { episodeSien = episodeSien.sien; }
    if (typeof episodeSien === 'undefined') { episodeSien = 'latest'; }

    return makeShowKey(showId) + ':episode:' + episodeSien;
  }

  function makeEpisodesKey (showId) {
    return makeShowKey(showId) + ':episodes';
  }

  function makeSubscriptionsKey (userId) {
    return makeUserKey(userId) + ':subscriptions';
  }

  function makeSubscribersKey (showId) {
    return makeShowKey(showId) + ':subscribers'
  }

  function makeUserKey (userId) {
    return 'user:' + userId
  }

  function makeFeedKey(userId) {
    return makeUserKey(userId) + ':feed';
  }

  function makeLogKey (type, time) {
    return 'log:' + type + ':' + padTime(time);
  }

  function makeActiveShowsKey () {
    return 'shows:active';
  }

  function makeInactiveShowsKey () {
    return 'shows:inactive';
  }

  function makeYoFailCountKey (userId) {
    return makeUserKey(userId) + ':yo-fail-count';
  }

  function close () {
    client.quit();
  }

  function addTimestamp (obj) {
    return getTime()
      .then(function (time) {
        obj.timestamp = time;
        return obj;
      });
  }

  function saveShow (show, callback) {
    var saveDetails = function (show) {
      var key = makeShowKey(show);
      show = removeNonScalars(show);

      return client.hmsetAsync(key, show)
        .return(show);
    };

    var removeFromWrongSet = function (show) {
      var remKey = show.active ? makeInactiveShowsKey() : makeActiveShowsKey();

      return client.sremAsync(remKey, show.id)
        .return(show);
    };

    var addToRightSet = function (show) {
      var addKey = show.active ? makeActiveShowsKey() : makeInactiveShowsKey();

      return client.saddAsync(addKey, show.id)
        .return(show);
    };

    return addTimestamp(show)
      .then(saveDetails)
      .then(removeFromWrongSet)
      .then(addToRightSet)
      .nodeify(callback);
  }

  function saveEpisode (showId, episode, callback) {
    showId = typeof showId === 'object' ? showId.id : showId;

    var addShowId = function (showId) {
      return function (episode) {
        episode.show_id = showId;
        return episode;
      };
    };

    var saveDetails = function (episode) {
      var key = makeEpisodeKey(episode.show_id, episode);

      return client.existsAsync(key)
        .then(function (exists) {
          if (exists) { return episode; }

          return client.hmsetAsync(key, episode)
            .return(episode);
        });
    };

    var addToSet = function (episode) {
      var episodeKey = makeEpisodeKey(episode.show_id, episode);
      var key = makeEpisodesKey(showId);

      return client.saddAsync(key, episodeKey)
        .return(episode);
    };

    return Promise.resolve(episode)
      .then(addShowId(showId))
      .then(addTimestamp)
      .then(saveDetails)
      .then(addToSet)
      .nodeify(callback);
  }

  function saveLatestEpisode (showId, episodeSien, callback) {
    var key = makeEpisodeKey(showId);
    var episodeKey = makeEpisodeKey(showId, episodeSien);

    return client.hgetallAsync(episodeKey)
      .then(function (episode) {
        return client.hmsetAsync(key, episode)
          .return(episode);
      })
      .nodeify(callback);
  }

  function getShow (showId, callback) {
    var key = makeShowKey(showId);

    return client.hgetallAsync(key)
      .nodeify(callback);
  }

  function getLatestEpisode (showId, callback) {
    var key = makeEpisodeKey(showId);

    return client.hgetallAsync(key)
      .nodeify(callback);
  }

  function getTime (callback) {
    return client.timeAsync()
      .then(function (t) {
        return (t[0] * 1000) + Math.round(t[1] / 1000);
      })
      .nodeify(callback);
  }

  function logScan (callback) {
    var writeLog = function (time) {
      var key = 'latest_scan:start';
      return client.setAsync(key, time);
    };

    return getTime()
      .then(writeLog)
      .nodeify(callback);
  }

  function getActiveShowIds (callback) {
    return client.smembersAsync(makeActiveShowsKey())
      .nodeify(callback);
  }

  function getInactiveShowIds (callback) {
    return client.smembersAsync(makeInactiveShowsKey())
      .nodeify(callback);
  }

  function createActiveShowsStream () {
    var stream = through2.obj();

    function writeData () {
      getActiveShowIds(function (err, showIds) {
        if (err) { return stream.end(); }

        showIds.forEach(function (showId) {
          stream.write({
            imdb_id: showId
          });
        });

        stream.end();
      });
    }

    process.nextTick(writeData);

    return stream;
  }

  function log (type, data, callback) {
    var writeLog = function (time) {
      var key = makeLogKey(type, time);
      data.time = time;

      return client.hmsetAsync(key, data);
    };

    return getTime()
      .then(writeLog)
      .nodeify(callback);
  }

  function getSubscriptions (userId, callback) {
    var key = makeSubscriptionsKey(userId);

    return client.smembersAsync(key)
      .nodeify(callback);
  }

  function getSubscribers (showId, callback) {
    var key = makeSubscribersKey(showId);

    return client.smembersAsync(key)
      .nodeify(callback);
  }

  function subscribeShow (userId, showId, callback) {
    var subscribe = function () {
      var subscriptionsKey = makeSubscriptionsKey(userId);
      var subscribersKey = makeSubscribersKey(showId);

      var multi = Promise.promisifyAll(client.multi());

      multi.sadd(subscribersKey, userId);
      multi.sadd(subscriptionsKey, showId);

      return multi.execAsync();
    };

    return subscribe(userId, showId)
      .then(addLatestEpisodeToFeed.bind(null, userId, showId))
      .nodeify(callback);
  }

  function unsubscribeShow (userId, showId, callback) {
    var unsubscribe = function (userId, showId) {
      var subscriptionsKey = makeSubscriptionsKey(userId);
      var subscribersKey = makeSubscribersKey(showId);

      var multi = Promise.promisifyAll(client.multi());

      multi.srem(subscribersKey, userId);
      multi.srem(subscriptionsKey, showId);

      return multi.execAsync();
    };

    return unsubscribe(userId, showId)
      .then(removeShowFromFeed.bind(null, userId, showId))
      .nodeify(callback);
  }

  function removeShowFromFeed (userId, showId, callback) {
    var feedKey = makeFeedKey(userId);
    var showKey = makeShowKey(showId);

    client.smembersAsync(feedKey)
      .then(function (episodeKeys) {
        var multi = Promise.promisifyAll(client.multi());

        episodeKeys = episodeKeys.filter(function (key) {
          return key.indexOf(showKey) !== -1;
        });

        episodeKeys.forEach(function (episodeKey) {
          multi.srem(feedKey, episodeKey);
        });

        return multi.execAsync();
      })
      .nodeify(callback);
  }

  function removeItemFromFeed (userId, showId, sien, callback) {
    var feedKey = makeFeedKey(userId);
    var episodeKey = makeEpisodeKey(showId, sien);

    return client.sremAsync(feedKey, episodeKey)
      .nodeify(callback);
  }

  function addLatestEpisodeToFeed (userId, show, callback) {
    return getLatestEpisode(show)
      .then(function (episode) {
        if (!episode) { return Promise.resolve(); }

        var episodeKey = makeEpisodeKey(show, episode);
        var feedKey = makeFeedKey(userId);

        return client.saddAsync(feedKey, episodeKey);
      })
      .nodeify(callback);
  }

  function getFeed(userId, callback) {
    var getFeedItemKeys = function (userId) {
      var feedKey = makeFeedKey(userId);

      return client.smembersAsync(feedKey);
    };

    var getEpisodeByKey = function (episodeKey) {
      return client.hgetallAsync(episodeKey);
    };

    var getFeedItem = function (episodeKey) {
      return getEpisodeByKey(episodeKey)
        .then(function (episode) {
          return getShow(episode.show_id)
            .then(function (show) {
              return {
                show_id: show.id,
                imdb_id: show.imdb_id,
                title: show.title,
                episide_title: episode.title,
                sien: episode.sien,
                season: episode.season,
                episode: episode.episode,
                poster: show.poster,
                first_aired: episode.first_aired,
                timestamp: episode.timestamp
              };
            });
        });
    };

    var filterAndSort = function (feedItems) {
      return feedItems
        .filter(function (result) {
          return !!result;
        })
        .sort(function (a, b) {
          var ats = a.timestamp || a.first_aired;
          var bts = b.timestamp || b.first_aired;
          return bts - ats;
        });
    };

    return getFeedItemKeys(userId)
      .map(getFeedItem)
      .then(filterAndSort)
      .nodeify(callback);
  }

  function createToken (length, callback) {
    return new Promise(function (resolve, reject) {
      crypto.randomBytes(length, function (err, token) {
        if (err) { return reject(err); }
        if (!token) { return reject(new Error('Failed to generate token')); }

        resolve(token.toString('hex'));
      });
    }).nodeify(callback);
  }

  function incrementYoFailCount (userId, callback) {
    var key = makeYoFailCountKey(userId);
    return client.incrAsync(key)
      .nodeify(callback);
  }

  function resetYoFailCount (userId, callback) {
    var key = makeYoFailCountKey(userId);
    return client.delAsync(key)
      .nodeify(callback);
  }

  return {
    saveShow: saveShow,
    saveEpisode: saveEpisode,
    saveLatestEpisode: saveLatestEpisode,
    getShow: getShow,
    getLatestEpisode: getLatestEpisode,
    getActiveShowIds: getActiveShowIds,
    getInactiveShowIds: getInactiveShowIds,
    createActiveShowsStream: createActiveShowsStream,
    log: log,
    getSubscribers: getSubscribers,
    getSubscriptions: getSubscriptions,
    subscribeShow: subscribeShow,
    unsubscribeShow: unsubscribeShow,
    removeItemFromFeed: removeItemFromFeed,
    addLatestEpisodeToFeed: addLatestEpisodeToFeed,
    getFeed: getFeed,
    getTime: getTime,
    logScan: logScan,
    incrementYoFailCount: incrementYoFailCount,
    resetYoFailCount: resetYoFailCount,
    close: close
  };
}
