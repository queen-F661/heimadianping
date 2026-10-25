-- 1.1 优惠卷id
local voucherId = ARGV[1]
-- 1.2 用户id
local userId = ARGV[2]

-- 2.数据key
-- 2.1 库存key:按"券"维度,一张券一个库存
local stockkey = 'seckill:stock:' .. voucherId
-- 2.2 订单key:也按"券"维度,set 里装的是下过单的 userId
--     排雷:这里千万别拼 userId,否则 set 里存的是 voucherId、下面却拿 userId 去查,永远查不到
local orderkey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1 判断库存是否充足 get stockkey
local stock = tonumber(redis.call("get", stockkey))
-- 排雷:key 不存在时 get 返回 false,tonumber(false) 得到 nil
--      而 nil 无法和数字比较,不先挡掉就会报 attempt to compare nil with number
-- 排雷:必须是 <= 0,不是 < 0。库存为 0 也不能下单,写成 < 0 会扣成 -1 造成超卖
if (stock == nil or stock <= 0) then
    -- 库存不足 返回1
    return 1
end

-- 3.2 判断当前用户是否下过单 SISMEMBER orderkey userId
--     这一步就是"一人一单"的全部实现:查这张券的买家名单里有没有我
if(redis.call("sismember",orderkey,userId) == 1) then
    -- 存在,说明是重复下单,返回2
    return 2
end

-- 3.3 扣库存 incrby stockkey -1
redis.call("incrby",stockkey,-1)
-- 3.4 下单(保存用户id) sadd orderkey userId
redis.call("sadd",orderkey,userId)
-- 3.5 到此 redis 的活干完了。阻塞队列版不需要 xadd,
--     订单由请求线程自己塞进 Java 内存队列 orderTasks
return 0
