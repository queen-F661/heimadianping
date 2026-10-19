import org.springframework.data.redis.core.script.DefaultRedisScript;

private static final DefaultRedisScript<Long> UNLOCK_SCRIPT =
        new DefaultRedisScript<Long>(
                "if redis.call('get', KEYS[1]) == ARGV[1] " +
                        "then return redis.call('del', KEYS[1]) " +
                        "else return 0 end",
                Long.class
        );