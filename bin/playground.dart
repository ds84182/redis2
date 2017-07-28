import 'dart:async';

import 'package:redis2/client.dart';
import 'package:redis2/connection.dart';
import 'package:redis2/pubsub.dart';

Future main() async {
  var conn = await RedisConnection.connect('localhost');
  var client = new RedisClient(conn);

  print(await client.get('foo'));
  await client.incrby('foo', 8);
  print(await client.get('foo'));

  await conn.clone().then((conn) async {
    var pubsub = new RedisPubSub(conn);
    print(await pubsub.subscribe(const ['MEMES']));
    print(await pubsub.patternSubscribe(const ['D?NK']));
    StreamSubscription<RedisMessage> subscription;
    subscription = pubsub.listen((message) {
      print("Got message ${message.message} on channel ${message.channel} (subscription ${message.subscription})");

      if (message.message == "Done!") {
        subscription.cancel();
        pubsub.connection.close();
      }
    });
    await client.publish('MEMES', "Hey");
    await client.publish('DANK', "denk");
    await client.publish('DENK', "dank");
    await client.publish('MEMES', "Done!");
  });

  await conn.close();
}