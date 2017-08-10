library redis2.client;

import 'dart:async';

import 'connection.dart' show RedisConnection;
import 'error.dart' show RedisError;

export 'src/script.dart' show RedisScript, RedisScriptSet;

class RedisClient {
  final RedisConnection connection;

  RedisClient(this.connection);

  Future<T> _wrap<T>(List<String> command) async {
    var result = await connection.send(command);

    if (result is RedisError) {
      throw result;
    }

    return result;
  }

  Future select(int db) => _wrap(['SELECT', db.toString()]);

  Future<String> scriptLoad(String script) => _wrap(['SCRIPT', 'LOAD', script]);

  Future<List<bool>> scriptExists(Iterable<String> hashes) =>
      _wrap<List<int>>(['SCRIPT', 'EXISTS']..addAll(hashes)).then((intList) {
        return intList.map((i) => i != 0).toList(growable: false);
      });

  Future<T> evalSha<T>(String hash,
          {Iterable<String> keys = const [],
          Iterable<String> args = const []}) =>
      _wrap(['EVALSHA', hash, keys.length.toString()]
        ..addAll(keys)
        ..addAll(args));

  static bool _isOk(value) => value == "OK";

  Future<bool> set(String key, String value,
      {Duration expires, bool ifNotExists: false, bool ifExists: false}) {
    final cmd = ['SET', key, value];

    if (expires != null) {
      cmd.add("PX");
      cmd.add(expires.inMilliseconds.toString());
    }

    assert(!(ifNotExists && ifExists));

    if (ifNotExists) {
      cmd.add("NX");
    } else if (ifExists) {
      cmd.add("XX");
    }

    return _wrap(cmd).then(_isOk);
  }

  Future<String> get(String key) => _wrap(['GET', key]);

  Future<String> getSet(String key, String value) =>
      _wrap(['GETSET', key, value]);

  Future hset(String key, String field, String value) =>
      _wrap(['HSET', key, field, value]);

  Future<String> hget(String key, String field) => _wrap(['HGET', key, field]);

  Future<int> incr(String key) => _wrap(['INCR', key]);

  Future<int> incrby(String key, int by) =>
      _wrap(['INCRBY', key, by.toString()]);

  Future<int> decr(String key) => _wrap(['DECR', key]);

  Future<int> decrby(String key, int by) =>
      _wrap(['DECRBY', key, by.toString()]);

  Future<List<String>> lrange(String key, int i, int j) =>
      _wrap(['LRANGE', key, i.toString(), j.toString()]);

  Future<int> publish(String channel, String message) =>
      _wrap(['PUBLISH', channel, message]);

  Future<bool> exists(String key) =>
      _wrap(['EXISTS', key]).then((int value) => value == 1);

  Future quit() => _wrap(const ['QUIT']);
}
