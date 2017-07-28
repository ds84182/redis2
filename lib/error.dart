library redis2.error;

class RedisError extends Error {
  final String message;

  RedisError(this.message);

  @override
  String toString() => "RedisError: $message";

  @override
  bool operator ==(other) => other is RedisError && other.message == message;

  @override
  int get hashCode => message.hashCode;
}
