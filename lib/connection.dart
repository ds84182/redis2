/// This library implements a basic connection to a Redis server.
/// It does not include a high level Redis wrapper. See [redis2.client] for that.
library redis2.connection;

import 'dart:async';
import 'dart:io';

import 'package:redis2/resp/decoder.dart';
import 'package:redis2/resp/encoder.dart';
import 'package:async/async.dart';

typedef bool RedisConnectionFilter(value);

class RedisConnection {
  final Socket _sock;
  StreamQueue _sockQueue;
  RedisConnectionFilter filter;

  RedisConnection._(this._sock) {
    var stream = _sock.transform(const RESPDecoder());

    // TODO: Optimize this by allowing the Socket stream subscription to react
    // to pause and resume events IFF there is no filter
    //if (filter != null) {
      var controller = new StreamController(sync: true);
      var sub = stream.listen((value) {
        if (filter == null || !filter(value)) {
          controller.add(value);
        }
      });
      controller.onCancel = () => sub.cancel();
      stream = controller.stream;
    //}

    _sockQueue = new StreamQueue(stream);
  }

  static Future<RedisConnection> connect(host, [int port = 6379]) async {
    var sock = await Socket.connect(host, port);
    sock.setOption(SocketOption.tcpNoDelay, true);
    return new RedisConnection._(sock);
  }

  Future send(List<String> command) {
    _sock.add(RESPEncoder.encode(command));
    return _sockQueue.next;
  }

  Future<RedisConnection> clone() {
    return connect(_sock.remoteAddress.address, _sock.remotePort);
  }

  Future close() => _sock.close();
}
