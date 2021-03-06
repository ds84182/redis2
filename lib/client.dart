library redis2.client;

import 'dart:async';

import 'connection.dart' show RedisConnection;
import 'error.dart' show RedisError;

export 'src/script.dart' show RedisScript, RedisScriptSet;

class RedisClient {
  final RedisConnection connection;

  RedisClient(this.connection);

  Future<T> _wrap<T>(List<String> command) async {
    final result = await connection.send(command);

    if (result is RedisError) {
      throw result;
    }

    return result as T;
  }

  static bool _countToBool(int count) => count != 0;

  // Connection

  Future<void> quit() => _wrap(const ['QUIT']);

  Future<void> select(int db) => _wrap(['SELECT', db.toString()]);

  // General key utilities

  Future<bool> exists(String key) =>
      _wrap<int>(['EXISTS', key]).then(_countToBool);

  Future<bool> del(String key) => _wrap<int>(['DEL', key]).then(_countToBool);

  Future<int> delMultiple(Iterable<String> keys) =>
      _wrap(['DEL']..addAll(keys));

  Future<String> type(String key) => _wrap(['TYPE', key]);

  Future<bool> expire(String key, Duration duration) =>
      _wrap<int>(['EXPIRE', key, duration.inSeconds.toString()])
          .then(_countToBool);

  Future<bool> expireAt(String key, DateTime timestamp) => _wrap<int>([
        'EXPIREAT',
        key,
        (timestamp.millisecondsSinceEpoch ~/ Duration.millisecondsPerSecond)
            .toString()
      ]).then(_countToBool);

  // Script utilities

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

  // String based keys

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

  // Hash tables

  Future<void> hset(String key, String field, String value) =>
      _wrap(['HSET', key, field, value]);

  Future<String> hget(String key, String field) => _wrap(['HGET', key, field]);

  // Integer-like string keys

  Future<int> incr(String key) => _wrap(['INCR', key]);

  Future<int> incrby(String key, int by) =>
      _wrap(['INCRBY', key, by.toString()]);

  Future<int> decr(String key) => _wrap(['DECR', key]);

  Future<int> decrby(String key, int by) =>
      _wrap(['DECRBY', key, by.toString()]);

  // Integer-like hash table values

  Future<int> hincr(String key, String field) => _wrap(['HINCR', key, field]);

  Future<int> hincrby(String key, String field, int by) =>
      _wrap(['HINCRBY', key, field, by.toString()]);

  Future<int> hdecr(String key, String field) => _wrap(['HDECR', key, field]);

  Future<int> hdecrby(String key, String field, int by) =>
      _wrap(['HDECRBY', key, field, by.toString()]);

  // Lists

  Future<String> lpop(String key) => _wrap(['LPOP', key]);

  Future<String> blpop(String key, {int timeout}) => _wrap<List<String>>(
          ['BLPOP', key, timeout == null ? "0" : timeout.toString()])
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

  Future<String> brpop(String key, {int timeout}) => _wrap<List<String>>(
          ['BRPOP', key, timeout == null ? "0" : timeout.toString()])
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

  // PubSub

  Future<int> publish(String channel, String message) =>
      _wrap(['PUBLISH', channel, message]);

  // Sets

  Future<bool> sadd(String key, String member) =>
      _wrap<int>(['SADD', key, member]).then(_countToBool);

  Future<int> saddMultiple(String key, Iterable<String> members) =>
      _wrap(['SADD', key]..addAll(members));

  Future<bool> sismember(String key, String member) =>
      _wrap<int>(['SISMEMBER', key, member]).then(_countToBool);

  Future<bool> srem(String key, String member) =>
      _wrap<int>(['SREM', key, member]).then(_countToBool);

  Future<int> sremMultiple(String key, Iterable<String> members) =>
      _wrap(['SREM', key]..addAll(members));
}
