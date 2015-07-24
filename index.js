var debug = require('debug')('yomypopcorn:dbclient');
var path = require('path');
var crypto = require('crypto');
var async = require('async');
var redis = require('redis');
var extend = require('object-assign');
var through2 = require('through2');
var utils = require('yomypopcorn-utils');

exports = module.exports = db;

var padTime = utils.padTime;
var cb = utils.cb;

function removeNonScalars (obj) {
    function replacer (key, value) {
      if (key && typeof value === 'object') { return undefined; }
      return value;
    }

    return JSON.parse(JSON.stringify(obj, replacer));
}

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

  function close () {
    client.quit();
  }

  function saveShow (show, callback) {
    getTime(function (err, time) {
      if (err) { return cb(callback, err); }

      function saveDetails (show, callback) {
        var key = makeShowKey(show);
        var show = removeNonScalars(show);
        show.timestamp = time;

        client.hmset(key, show, function (err) {
          cb(callback, err, show);
        });
      }

      function addToSet (show, callback) {
        if (!show.active) {
          client.srem('shows:active', show.id, function () {
            client.sadd('shows:inactive', show.id, function (err) {
              cb(callback, err);
            });
          });

        } else {
          client.srem('shows:inactive', show.id, function () {
            client.sadd('shows:active', show.id, function (err) {
              cb(callback, err);
            });
          });
        }
      }

      async.parallel([
        function (callback) {
          saveDetails(show, callback);
        },
        function (callback) {
          addToSet(show, callback);
        }

      ], function (err) {
        if (err) { return cb(callback, err); }

        cb(callback, null, show);
      });
    });

  }

  function saveEpisode (showId, episode, callback) {
    showId = typeof showId === 'object' ? showId.id : showId;

    getTime(function (err, time) {
      if (err) { return cb(callback, err); }

      function saveDetails (showId, episode, callback) {
        var key = makeEpisodeKey(showId, episode);

        client.exists(key, function (err, exists) {
          if (err || exists) { return cb(callback); }

          episode = removeNonScalars(episode);

          episode.show_id = showId;
          episode.timestamp = time;

          client.hmset(key, episode, function (err) {
            cb(callback, err, episode);
          });
        });

      }

      function addToSet (showId, episode, callback) {
        var episodeKey = makeEpisodeKey(showId, episode);
        var key = makeEpisodesKey(showId);

        client.sadd(key, episodeKey, function (err) {
          cb(callback, err);
        });
      }

      async.parallel([
        function (callback) {
          saveDetails(showId, episode, callback);
        },
        function (callback) {
          addToSet(showId, episode, callback);
        }

      ], function (err) {
        if (err) { return cb(callback, err); }

        cb(callback, null);
      });
    });
  }

  function saveLatestEpisode (showId, episodeSien, callback) {
    var key = makeEpisodeKey(showId);
    var episodeKey = makeEpisodeKey(showId, episodeSien);

    client.hgetall(episodeKey, function (err, episode) {
      if (err) { return cb(callback, err); }

      client.hmset(key, episode, function (err) {
        if (err) { return cb(callback, err); }

        cb(callback, err);
      });
    });
  }

  function getShow (imdbId, callback) {
    var key = 'show:' + imdbId;
    client.hgetall(key, function (err, show) {
      cb(callback, err, show);
    });
  }

  function getLatestEpisode (showId, callback) {
    var key = makeEpisodeKey(showId);

    client.hgetall(key, function (err, episode) {
      cb(callback, err, episode);
    });
  }

  function getTime (callback) {
    client.time(function (err, t) {
      if (err) { return cb(callback, err); }

      var time = (t[0] * 1000) + Math.round(t[1] / 1000);
      cb(callback, null, time);
    });
  }

  function logScan (callback) {
    getTime(function (err, time) {
      if (err) { return cb(callback, err); }

      client.set('latest_scan:start', time, function (err) {
        cb(callback, err);
      });
    });
  }

  function getActiveShowIds (callback) {
    client.smembers('shows:active', function (err, ids) {
      cb(callback, err, ids);
    });
  }

  function getInactiveShowIds (callback) {
    client.smembers('shows:inactive', function (err, ids) {
      cb(callback, err, ids);
    });
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
    getTime(function (err, time) {
      if (err) { return cb(callback, err); }

      var key = 'log:' + type + ':' + padTime(time);

      data.time = time;

      client.hmset(key, data, function (err) {
        cb(callback, err, data)
      });
    });
  }

  function getSubscriptions (userId, callback) {
    var key = makeSubscriptionsKey(userId);
    client.smembers(key, function (err, subscriptions) {
      cb(callback, err, subscriptions);
    });
  }

  function getSubscribers (showId, callback) {
    var key = makeSubscribersKey(showId);

    client.smembers(key, function (err, subscribers) {
      cb(callback, err, subscribers);
    });
  }

  function subscribeShow (userId, showId, callback) {
    var subscriptionsKey = makeSubscriptionsKey(userId);
    var subscribersKey = makeSubscribersKey(showId);

    var multi = client.multi();

    multi.sadd(subscribersKey, userId);
    multi.sadd(subscriptionsKey, showId);

    multi.exec(function (err) {
      if (err) { return cb(callback, err); }

      addLatestEpisodeToFeed(userId, showId, function (err) {
        cb(callback, err);
      });
    });
  }

  function unsubscribeShow (userId, showId, callback) {
    var subscriptionsKey = makeSubscriptionsKey(userId);
    var subscribersKey = makeSubscribersKey(showId);

    var multi = client.multi();

    multi.srem(subscribersKey, userId);
    multi.srem(subscriptionsKey, showId);

    multi.exec(function (err) {
      removeShowFromFeed(userId, showId, function () {
        cb(callback, err);
      });
    });
  }

  function removeShowFromFeed (userId, showId, callback) {
    var feedKey = makeFeedKey(userId);
    var showKey = makeShowKey(showId);

    client.smembers(feedKey, function (err, episodeKeys) {
      if (err) { return cb(callback, err); }

      episodeKeys = episodeKeys.filter(function (key) {
        return key.indexOf(showKey) !== -1;
      });

      var multi = client.multi();
      episodeKeys.forEach(function (episodeKey) {
        multi.srem(feedKey, episodeKey);
      });

      multi.exec(function (err) {
        cb(callback, err);
      });
    });
  }

  function addLatestEpisodeToFeed (userId, show, callback) {
    getLatestEpisode(show, function (err, episode) {
      if (err) { return cb(callback, err); }

      var episodeKey = makeEpisodeKey(show, episode);
      var feedKey = makeFeedKey(userId);

      client.sadd(feedKey, episodeKey, function (err) {
        cb(callback, err);
      });
    });
  }

  function getFeed(userId, callback) {
    var feedKey = makeFeedKey(userId);

    client.smembers(feedKey, function (err, feedItemKeys) {
      if (err) { return db(callback, null, []); }

      async.map(
        feedItemKeys,

        function (feedItemKey, callback) {
          client.hgetall(feedItemKey, function (err, episode) {
            if (err) { return callback(err); }
            if (!episode) { return callback(err); }

            getShow(episode.show_id, function (err, show) {
              if (err) { return callback(err); }

              callback(null, {
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
              });
            });
          });
        },

        function (err, results) {
          // filter out null values
          results = results.filter(function (result) {
            return !!result;
          })
          .sort(function (a, b) {
            return b.sien - a.sien;
          });

          cb(callback, null, results || []);
        }
      );
    });
  }

  function createToken (length, callback) {
    crypto.randomBytes(length, function (err, token) {
      if (err) { return cb(callback, err); }
      if (!token) { return cb(new Error('Failed to generate token')); }

      cb(callback, null, token.toString('hex'));
    });
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
    addLatestEpisodeToFeed: addLatestEpisodeToFeed,
    getFeed: getFeed,
    getTime: getTime,
    logScan: logScan,
    close: close
  };
}
