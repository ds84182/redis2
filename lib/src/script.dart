library redis2.src.script;

import 'dart:async';
import 'dart:convert' show utf8;

import 'package:crypto/crypto.dart' show sha1;
import 'package:meta/meta.dart';
import 'package:redis2/client.dart' show RedisClient;

class RedisScript {
  final String identifier;
  final String script;
  String _calculatedHash;

  RedisScript(this.identifier, this.script);

  String get hash =>
      _calculatedHash ??= sha1.convert(utf8.encode(script)).toString();

  Future load(RedisClient client) => client.scriptLoad(script);

  Future<T> eval<T>(
    RedisClient client, {
    Iterable<String> keys = const [],
    Iterable<String> args = const [],
  }) =>
      client.evalSha<T>(hash, keys: keys, args: args);
}

class RedisScriptSet {
  final List<RedisScript> _scripts;

  Map<String, RedisScript> _identifierMapCache;

  RedisScriptSet(Iterable<RedisScript> scripts)
      : _scripts = scripts.toList(growable: false);
  RedisScriptSet.single(RedisScript script)
      : _scripts = new List<RedisScript>(1)..[0] = script;

  Map<String, RedisScript> get _identifierMap =>
      _identifierMapCache ??= new Map<String, RedisScript>.fromEntries(_scripts
          .map((RedisScript script) => MapEntry(script.identifier, script)));

  Iterable<RedisScript> _filterScriptsByExistence(List<bool> existence,
      {@required bool exists}) sync* {
    for (int i = 0; i < existence.length; i++) {
      if (existence[i] != exists) {
        yield _scripts[i];
      }
    }
  }

  Future<int> ensureExists(RedisClient client) async {
    final existence =
        await client.scriptExists(_scripts.map((script) => script.hash));

    // Bail early if everything exists already
    if (existence.every((exists) => exists)) return 0;

    final results = await Future.wait(
      _filterScriptsByExistence(existence, exists: false)
          .map((script) => script.load(client)),
      eagerError: true,
    );

    return results.length;
  }

  RedisScript operator [](String identifier) => _identifierMap[identifier];
}
