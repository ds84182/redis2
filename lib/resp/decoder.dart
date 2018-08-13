library redis2.resp.parser;

import 'dart:async';
import 'dart:collection';

import 'package:charcode/ascii.dart';

import 'package:redis2/error.dart';

const _kTerminator = '\r\n';

class _AsyncNotification {
  Completer _completer;

  Future get wait => (_completer ??= new Completer()).future;

  void notify() {
    _completer?.complete();
    _completer = null;
  }
}

class _StreamBuffer<T> {
  final _buffers = new Queue<List<T>>();
  final _notification = new _AsyncNotification();

  var _index = 0;
  bool _done = false;

  _StreamBuffer(Stream<List<T>> stream) {
    stream.listen((list) {
      _buffers.add(list);
      _notification.notify();
    }, onDone: () {
      _done = true;
      _notification.notify();
    });
  }

  FutureOr<T> get next {
    if (_buffers.isEmpty) {
      if (_done) return null;
      return _notification.wait.then((_) => next);
    }

    var buf = _buffers.first;
    var data = buf[_index++];

    if (_index >= buf.length) {
      _buffers.removeFirst();
      _index = 0;
    }

    return data;
  }

  bool get done => _buffers.isEmpty && _done;

  FutureOr<List<T>> take(int count) async {
    var out = <T>[];

    // print("take idx $_index");

    do {
      // print("bufferiter ${out.length} $count");
      if (_buffers.isEmpty) await _notification.wait;

      var oldlen = out.length;
      out.addAll(_buffers.first.skip(_index).take(count - out.length));
      _index += out.length - oldlen;

      if (_index >= _buffers.first.length) {
        _buffers.removeFirst();
        _index = 0;
      }
    } while (out.length < count);

    // print("take ${out.length} $count");

    out.length = count;
    return out;
  }
}

class RESPDecoder extends StreamTransformerBase<List<int>, dynamic> {
  const RESPDecoder();

  @override
  Stream bind(Stream<List<int>> stream) {
    var controller = new StreamController(sync: true);
    var arrays = <List>[];

    void add(val) {
      if (arrays.isNotEmpty) {
        var arr = arrays.last;
        (arr[1] as List).add(val);
        arr[0]--;

        if (arr[0] == 0) {
          arrays.removeLast();
          add(arr[1]);
        }
      } else {
        controller.add(val);
      }
    }

    () async {
      var queue = new _StreamBuffer(stream);

      while (!queue.done) {
        var byte = await queue.next;

        if (byte == null) continue;

        switch (byte) {
          case $minus:
          case $plus:
          case $colon:
          case $dollar:
          case $asterisk:
            {
              // Simple string, take until we get to \r\n
              var buffer = new StringBuffer();
              // TODO: UTF8 here
              int a, b;
              while (a != $cr && b != $lf) {
                a = b;
                b = await queue.next;
                buffer.writeCharCode(b);
              }

              var data = buffer.toString().substring(0, buffer.length - 2);

              // print(new String.fromCharCode(byte) + " " + data);

              if (byte == $plus) {
                add(data);
              } else if (byte == $minus) {
                add(new RedisError(data));
              } else {
                int i = int.parse(data);

                if (byte == $dollar) {
                  if (i > 0) {
                    // Read a bulk string
                    // TODO: UTF8 here
                    add(new String.fromCharCodes(await queue.take(i)));

                    // Skip last two
                    await queue.next;
                    await queue.next;
                  } else if (i == 0) {
                    add("");

                    // Skip last two
                    await queue.next;
                    await queue.next;
                  } else {
                    add(null);
                  }
                } else if (byte == $asterisk) {
                  if (i > 0) {
                    // print("start array $i");
                    arrays.add([i, []]);
                  } else if (i == 0) {
                    add(const []);
                  } else {
                    add(null);
                  }
                } else {
                  add(i);
                }
              }
              break;
            }
          default:
            print("PROTOCOL ERROR " + new String.fromCharCode(byte));
            break;
        }
      }

      await controller.close();
    }();

    return controller.stream;
  }
}
