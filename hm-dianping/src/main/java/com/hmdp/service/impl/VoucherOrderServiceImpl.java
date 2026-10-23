package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final String KEY_PREFIX = "lock:";


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 线程运行开关,volatile 保证多线程可见
    private volatile boolean running = true;

    @PostConstruct
    private void init() {
        // 启动时先确保队列和消费者组存在,再开启后台线程
        initStreamGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 初始化 stream 队列和消费者组
     * 背景:XREADGROUP 要求 key 和 group 都已存在,否则报 NOGROUP 错误。
     * 而 lua 脚本里的 XADD 只会自动创建 key,不会创建 group,
     * 所以换了 Redis 环境或清空过数据后,必须重建消费者组。
     */
    private void initStreamGroup() {
        try {
            // 1. key 不存在时先补一条占位消息(XGROUP CREATE 要求 key 必须存在)
            if (Boolean.FALSE.equals(stringRedisTemplate.hasKey("stream.orders"))) {
                stringRedisTemplate.opsForStream().add("stream.orders", Collections.singletonMap("init", "1"));
            }
            // 2. 创建消费者组,从最新位置($)开始消费,占位消息不会被读到
            stringRedisTemplate.opsForStream().createGroup("stream.orders", ReadOffset.latest(), "g1");
            log.info("消费者组 g1 创建成功");
        } catch (Exception e) {
            // 组已存在时会报 BUSYGROUP,属正常情况,无需处理
            log.info("消费者组 g1 已存在,跳过创建");
        }
    }

    @PreDestroy
    private void destroy(){
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdown();
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    // 关停保险:项目关闭时 Redis 连接工厂已被销毁,再读只会无限报错,直接退出循环
                    if (!running) {
                        break;
                    }
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            // 注意:这里也必须用 running 判断,否则关停时会在这里死循环刷异常日志
            while (running) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    // 关停保险:项目关闭时 Redis 连接工厂已被销毁,直接退出,不再重试
                    if (!running) {
                        break;
                    }
                    // NOGROUP 保险:队列或消费组不存在(比如 Redis 被清空过),重试无意义,直接退出
                    if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                        log.error("队列或消费组不存在,停止处理 pending-list,重启项目会自动重建");
                        break;
                    }
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    createVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 查找当前库存为不为空
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        // 怕当前的用户没有查询出来
        if(voucher == null){
            return Result.fail("优惠卷不存在");
        }
        // 怕当前的时间进行不在一个区间
        // 这个是判断为当前设定的时间和我现在的时间进行比较
        // isAfter
        // 现在的时间在这个之前 就是true
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            // 尚未开始
            return Result.fail("秒杀尚未开始");
        }
        // 这个是相反的
        // 当前我现在的时间超过这个我设定的时间 就是true  就是LocalDateTime.now()比voucher.getEndTime()这个大
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }

        if (voucher.getStock() <= 0) {
            return Result.fail("当前的优惠卷已抢完");
        }
        Long user_Id = UserHolder.getUser().getId();

        // 1.获取当前锁对象
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + user_Id);
        boolean Lock = simpleRedisLock.tryLock(1200);
        if(!Lock){
            return Result.fail("当前优惠卷购买失败");
        }
        // intern()这个是直接去常量池里面进行查询 如果没有,第一次没有就直接插入 后面就直接在常量池进行查找
        // 获取动态代理对象
        // 因为 synchronized 块里 this.createVoucherOrder(voucherId)
        // 这种自己调用自己，是 this 直接调用，不经过代理，@Transactional 完全失效；
        // 锁释放了事务还没提交，
        // 别的线程能读到旧数据。所以必须用代理对象来调用，让事务拦截器介入
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            simpleRedisLock.unlock();
        }

    }

    /**
     * 锁如果放在这个方法上,他的锁就是这个类,会导致调用的地方不管是谁都要拿锁 效率太低
     * 不能根据当前用户进行锁 因为我们这里只需要根据当前的用户进行锁就行 那么效率就会高点
     * 单独使用结构体包裹 让当前用户id来进行判断 要是通过了就可以进 在下一次通过了在进
     * */
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 把当前的用户数据在UserHolder
        Long user_Id = UserHolder.getUser().getId();

        Integer count = query().eq("user_id", user_Id).eq("voucher_id", voucherId).count();
        if (count > 0){
            return Result.fail("当前用户已经下过单");
        }

        // 1.先扣减库存
        boolean sucess = seckillVoucherService.update().setSql("stock = stock - 1")  // set stock = stock -1
                .eq("voucher_id", voucherId)   // where voucher_id = 当前的秒杀卷id and stock = 刚刚查询的库存
                // cas解决方案
                // .eq("stock", voucher.getStock())
                // cas解决方案改进
                .gt("stock", 0)
                .update();
        // 假设当前的小于0 就会返回一个false 会扣减失败
        if (!sucess){
            return Result.fail("当前的优惠券已抢完");
        }

        // 在把当前的唯一生成id也拿过来
        RedisIdWorker redisIdWorker1 = new RedisIdWorker(stringRedisTemplate);
        long id = redisIdWorker1.nextId("order");

        // 初始化
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(id);
        voucherOrder.setUserId(user_Id);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setStatus(1);
        voucherOrder.setPayType(1);

        // 在把这个值添加进去
        save(voucherOrder);
        return Result.ok(id);

    }
}
