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

	function close () {
		client.quit();
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
		client.hgetall(key, function (err, show) {
			cb(callback, err, show);
		});
	}

	function getLatestEpisode (imdbId, callback) {
		var key = 'episode:' + imdbId;
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
				cb(callback, err, data)
			});
		});
	}

	function getUser (type, username, callback) {
		var key = 'user:' + type + ':' + username.toLowerCase();
		client.hgetall(key, function (err, user) {
			cb(callback, err, user);
		});
	}

	function getYoUser (username, callback) {
		getUser('yo', username, function (err, user) {
			cb(callback, err, user);
		});
	}

	function createUser (data, callback) {
		if (typeof data !== 'object') { return cb(callback, new Error('Missing data object')); }
		if (!data.type) { return cb(callback, new Error('Missing type')) }
		if (!data.username) { return cb(callback, new Error('Missing username')) }

		getUser(data.type, data.username, function (err, user) {
			if (user) { return callback(new Error('User already exists')); }

			getTime(function (err, time) {
				if (err) { return cb(callback, err); }

				var userId = data.type + ':' + data.username.toLowerCase();
				var key = 'user:' + userId;

				var userData = {
					type: data.type,
					username: data.username,
					createdAt: time
				};

				client.hmset(key, userData, function (err) {
					cb(callback, err, userData);
				});
			});
		});
	}

	function createYoUser (data, callback) {
		if (typeof data !== 'object') { return cb(callback, new Error('Missing data object')); }

		data.type = 'yo';

		createUser(data, function (err, user) {
			cb(callback, err, user);
		});
	}

	function createToken (length, callback) {
		crypto.randomBytes(length, function (err, token) {
			if (err) { return cb(callback, err); }
			if (!token) { return cb(new Error('Failed to generate token')); }

			cb(callback, null, token.toString('hex'));
		});
	}

	function createPin () {
		var min = 100000;
		var max = 999999;

		return Math.floor( Math.random() * ( max - min + 1 ) + min);
	}

	function createUserAuthToken (type, username, callback) {
		var TOKEN_EXPIRATION_TIME = 30000;

		getUser(type, username, function (err, user) {
			if (!user) { return cb(callback, new Error('User not found')); }

			createToken(32, function (err, token) {
				if (err) { return cb(callback, err); }

				getTime(function (err, time) {

					var tokenKey = 'userauthtoken:token:' + token;
					var userTokenKey = 'userauthtoken:' + type + ':' + username;
					var pin = createPin();

					var tokenData = {
						createdAt: time,
						token: token,
						pin: pin
					};

					var multi = client.multi();

					multi.set(userTokenKey, token);
					multi.pexpire(userTokenKey, TOKEN_EXPIRATION_TIME);

					multi.hmset(tokenKey, tokenData);
					multi.pexpire(tokenKey, TOKEN_EXPIRATION_TIME);

					multi.exec(function (err) {
						cb(callback, err, tokenData);
					});
				});
			});
		});
	}

	function getUserAuthToken (type, username, callback) {
		var userTokenKey = 'userauthtoken:' + type + ':' + username;

		client.get(userTokenKey, function (err, token) {
			if (err) { return cb(callback, err); }
			if (!token) { return cb(callback, new Error('No token')); }

			var tokenKey = 'userauthtoken:token:' + token;

			client.hgetall(tokenKey, function (err, tokenData) {
				if (!tokenData) { return cb(callback, new Error('No token')); }

				cb(callback, err, tokenData);
			});
		});
	}

	function deleteUserAuthToken (type, username, callback) {
		getUserAuthToken(type, username, function (err, tokenData) {
			if (err) { return cb(callback);}
			if (!tokenData) { return cb(callback);}

			var userTokenKey = 'userauthtoken:' + type + ':' + username;
			var tokenKey = 'userauthtoken:token:' + tokenData.token;

			client.del(userTokenKey);
			client.del(tokenKey);

			cb(callback);
		});
	}

	return {
		getYoUser: getYoUser,
		createYoUser: createYoUser,
		createUser: createUser,
		getUser: getUser,
		createUserAuthToken: createUserAuthToken,
		getUserAuthToken: getUserAuthToken,
		deleteUserAuthToken: deleteUserAuthToken,
		saveShow: saveShow,
		getShow: getShow,
		getLatestEpisode: getLatestEpisode,
		getActiveShowIds: getActiveShowIds,
		getInactiveShowIds: getInactiveShowIds,
		createActiveShowsStream: createActiveShowsStream,
		logEpisodeUpdate: logEpisodeUpdate,
		log: log,
		getTime: getTime,
		logScan: logScan,
		close: close
	};
}
