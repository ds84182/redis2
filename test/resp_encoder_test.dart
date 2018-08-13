import 'dart:async';
import 'dart:convert';

import 'package:test/test.dart';
import 'package:redis2/resp/encoder.dart';

const input = const [
  "HELLO",
  "WORLD",
  "",
  null,
  "\r\n!!\r\n"
];

const outputRESP =
    "*5\r\n"
    "\$5\r\nHELLO\r\n"
    "\$5\r\nWORLD\r\n"
    "\$0\r\n\r\n"
    "\$-1\r\n"
    "\$6\r\n\r\n!!\r\n\r\n";

Future main() async {
  test("encodes properly", () {
    expect(RESPEncoder.encode(input), utf8.encode(outputRESP));
  });
}
