library redis2.pubsub;

import 'dart:async';

import 'connection.dart';
import 'error.dart';

import 'package:async/async.dart';

class RedisMessage {
  final String message;
  final String channel;
  final String subscription;

  RedisMessage(this.message, this.channel, this.subscription);
}

class RedisPubSub extends DelegatingStream<RedisMessage> {
  final RedisConnection connection;
  final StreamController<RedisMessage> _controller;

  RedisPubSub._(this.connection, this._controller) : super(_controller.stream) {
    // We have to intercept "message" messages by adding a connection filter
    connection.filter = _filterMessages;
  }

  factory RedisPubSub(RedisConnection conn) =>
      new RedisPubSub._(conn, new StreamController());

  bool _filterMessages(value) {
    if (value is List<String> &&
        value.isNotEmpty &&
        value[0].endsWith('message')) {
      bool pattern = value[0] == 'pmessage';
      _controller.add(new RedisMessage(
          value.last, value[2], pattern ? value[1] : value[2]));
      return true;
    }
    return false;
  }

  Future<T> _wrap<T>(List<String> command) async {
    var result = await connection.send(command);

    if (result is RedisError) {
      throw result;
    }

    return result;
  }

  Future<int> subscribe(Iterable<String> channels) =>
      _wrap<List>(['SUBSCRIBE']..addAll(channels)).then((l) => l[2] as int);

  Future<int> patternSubscribe(Iterable<String> channels) =>
      _wrap<List>(['PSUBSCRIBE']..addAll(channels)).then((l) => l[2] as int);

  Future<int> unsubscribe(Iterable<String> channels) =>
      _wrap<List>(['UNSUBSCRIBE']..addAll(channels)).then((l) => l[2] as int);

  Future<int> punsubscribe(Iterable<String> channels) =>
      _wrap<List>(['PUNSUBSCRIBE']..addAll(channels)).then((l) => l[2] as int);

  Future quit() => _wrap(const ['QUIT']);
}
