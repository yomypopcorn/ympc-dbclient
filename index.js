var debug = require('debug')('yomypopcorn:dbclient');
var path = require('path');
var async = require('async');
var redis = require('redis');
var extend = require('object-assign');
var through2 = require('through2');
var utils = require('yomypopcorn-utils');

exports = module.exports = db;

var padTime = utils.padTime;
var cb = utils.cb;

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

	function saveShow (show, callback) {
		function saveDetails (show, callback) {
			var key = 'show:' + show.imdb_id;

			client.hmset(key, {
				imdb_id: show.imdb_id,
				active: show.active,
				title: show.title,
				synopsis: show.synopsis,
				year: show.year,
				country: show.country,
				network: show.network,
				rating: show.rating,
				poster: show.poster,
				fanart: show.fanart

			}, function (err) {
				cb(callback, err, show);
			});
		}

		function saveEpisode (show, callback) {
			if (!show.latestEpisode) {
				return cb(callback);
			}

			var key = 'episode:' + show.imdb_id;
			var episode = show.latestEpisode;

			client.hmset(key, {
				imdb_id: show.imdb_id,
				sien: episode.sien,
				season: episode.season,
				episode: episode.episode,
				title: episode.title,
				overview: episode.overview

			}, function (err) {
				cb(callback, err);
			});
		}

		function addToSet (show, callback) {
			if (!show.active) {
				client.srem('shows:active', show.imdb_id, function () {
					client.sadd('shows:inactive', show.imdb_id, function (err) {
						cb(callback, err);
					});
				});

			} else {
				client.srem('shows:inactive', show.imdb_id, function () {
					client.sadd('shows:active', show.imdb_id, function (err) {
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
				saveEpisode(show, callback);
			},
			function (callback) {
				addToSet(show, callback);
			}

		], function (err) {
			if (err) {
				return callback(err);
			}

			cb(callback, null, show);
		});
	}

	function getShow (imdbId, callback) {
		var key = 'show:' + imdbId;
		client.hgetall(key, callback);
	}

	function getLatestEpisode (imdbId, callback) {
		var key = 'episode:' + imdbId;
		client.hgetall(key, callback);
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

	function logEpisodeUpdate (update, callback) {
		getTime(function (err, time) {
			if (err) { return cb(callback, err); }

			var key = 'episode_update:' + padTime(time) + ':' + update.imdb_id;

			var data = {
				imdb_id: update.imdb_id,
				time: time,
				prev_season: update.prev_season,
				prev_episode: update.prev_episode,
				new_season: update.new_season,
				new_episode: update.new_episode

			};

			client.hmset(key, data, function (err) {
				cb(callback, err, data);
			});
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
				cb(err, data)
			});
		});
	}

	return {
		saveShow: saveShow,
		getShow: getShow,
		getLatestEpisode: getLatestEpisode,
		getActiveShowIds: getActiveShowIds,
		getInactiveShowIds: getInactiveShowIds,
		createActiveShowsStream: createActiveShowsStream,
		logEpisodeUpdate: logEpisodeUpdate,
		log: log,
		getTime: getTime,
		logScan: logScan
	};
}
