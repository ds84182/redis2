library redis2.resp.encoder;

import 'dart:convert';

import 'package:charcode/ascii.dart';

class RESPEncoder {
  static List<int> encode(List<String> value) {
    List<int> output = [];

    output.add($asterisk);
    output.addAll(ASCII.encode(value.length.toString()));
    output.addAll(const [$cr, $lf]);

    value.forEach((str) {
      if (str == null) {
        output.addAll(const [$dollar, $minus, $1, $cr, $lf]);
      } else {
        var encoded = str.codeUnits;
        output.add($dollar);
        output.addAll(ASCII.encode(encoded.length.toString()));
        output.addAll(const [$cr, $lf]);
        output.addAll(encoded);
        output.addAll(const [$cr, $lf]);
      }
    });

    return output;
  }
}
