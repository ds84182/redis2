import 'dart:async';
import 'dart:convert';

import 'package:test/test.dart';
import 'package:redis2/error.dart';
import 'package:redis2/resp/decoder.dart';

const inputRESP =
    "+OK\r\n"
    "-Error message\r\n"
    ":1234\r\n"
    "\$6\r\n\r\nabcd\r\n"
    "\$0\r\n\r\n"
    "\$-1\r\n"
    "*4\r\n"
    "*2\r\n"
    ":1203\r\n"
    ":1204\r\n"
    "*0\r\n"
    "*-1\r\n"
    ":1235\r\n"
;

final output = [
  "OK",
  new RedisError("Error message"),
  1234,
  "\r\nabcd",
  "",
  null,
  [[1203, 1204], [], null, 1235]
];

Future main() async {
  test("decodes properly", () {
    var result = new Stream
        .fromIterable(const [inputRESP])
        .transform(const Utf8Encoder())
        .transform(const RESPDecoder())
        .toList();

    expect(result, completion(output));
  });
}
