-- -- 锁的key keys数组
-- local key = KEYS[1]
-- -- 当前线程的数据
-- local threadId = ARGV[1]
if(redis.call("get",KEYS[1]) == ARGV[1]) then
    -- 释放锁
    return redis.call("del",KEYS[1])
end
return 0;