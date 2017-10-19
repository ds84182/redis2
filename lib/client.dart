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

  Future<int> hincr(String key, String field) => _wrap(['HINCR', key, field]);

  Future<int> hincrby(String key, String field, int by) =>
      _wrap(['HINCRBY', key, field, by.toString()]);

  Future<int> hdecr(String key, String field) => _wrap(['HDECR', key, field]);

  Future<int> hdecrby(String key, String field, int by) =>
      _wrap(['HDECRBY', key, field, by.toString()]);

  Future<String> lpop(String key) => _wrap(['LPOP', key]);

  Future<String> blpop(String key, {int timeout}) =>
      _wrap(['BLPOP', key, timeout == null ? "0" : timeout.toString()])
          .then((List<String> reply) => reply?.last);

  Future<List<String>> blpopMultiple(Iterable<String> keys, {int timeout}) =>
      _wrap(['BLPOP']
        ..addAll(keys)
        ..add(timeout == null ? "0" : timeout.toString()));

  Future<int> lpush(String key, String value, {bool ifExists: false}) =>
      _wrap([ifExists ? 'LPUSHX' : 'LPUSH', key, value]);

  Future<int> lpushMultiple(String key, Iterable<String> values) =>
      _wrap(['LPUSH', key]..addAll(values));

  Future<String> rpop(String key) => _wrap(['RPOP', key]);

  Future<String> brpop(String key, {int timeout}) =>
      _wrap(['BRPOP', key, timeout == null ? "0" : timeout.toString()])
          .then((List<String> reply) => reply?.last);

  Future<List<String>> brpopMultiple(Iterable<String> keys, {int timeout}) =>
      _wrap(['BRPOP']
        ..addAll(keys)
        ..add(timeout == null ? "0" : timeout.toString()));

  Future<int> rpush(String key, String value, {bool ifExists: false}) =>
      _wrap([ifExists ? 'RPUSHX' : 'RPUSH', key, value]);

  Future<int> rpushMultiple(String key, Iterable<String> values) =>
      _wrap(['RPUSH', key]..addAll(values));

  Future<String> rpoplpush(String source, String destination) =>
      _wrap(['RPOPLPUSH', source, destination]);

  Future<String> brpoplpush(String source, String destination, {int timeout}) =>
      _wrap([
        'BRPOPLPUSH',
        source,
        destination,
        timeout == null ? "0" : timeout.toString()
      ]);

  Future<List<String>> lrange(String key, int i, int j) =>
      _wrap(['LRANGE', key, i.toString(), j.toString()]);

  Future<int> publish(String channel, String message) =>
      _wrap(['PUBLISH', channel, message]);

  Future<bool> exists(String key) =>
      _wrap(['EXISTS', key]).then((int value) => value == 1);

  Future quit() => _wrap(const ['QUIT']);
}
