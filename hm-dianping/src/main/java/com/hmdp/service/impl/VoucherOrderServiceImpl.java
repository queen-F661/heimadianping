package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
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

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    // 事务代理:由请求线程赋值、消费线程读取,必须 volatile,否则消费线程可能永远读到 null
    private volatile IVoucherOrderService proxy;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 消息队列名 / 消费者组名 / 消费者名:统一提到类级别
    // 一是 init() 建组要用,二是避免这几处字符串各写各的又写错(之前注释里少个 s 就是这么来的)
    private static final String QUEUE_NAME = "stream.orders";
    private static final String GROUP_NAME = "g1";
    private static final String CONSUMER_NAME = "c1";

    // 单线程线程池:整个应用只有一个消费线程,保证消息顺序处理
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 线程运行开关,volatile 保证多线程可见
    private volatile boolean running = true;

    /**
     *  @PostConstruct 当前类初始化的时候进行加载
     * */
    @PostConstruct
    private void init() {
        // 顺序不能反:必须先把消费者组建出来,再启动消费线程
        // 反了的话线程一上来就 XREADGROUP,组还没建,直接 NOGROUP
        initConsumerGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 初始化消费者组。
     *
     * 为什么非写这段不可:Redis 的消费者组不会自动创建。
     *   XADD          -> 只建 stream,不建组
     *   XREADGROUP    -> 只读,不建组,组不存在直接抛 NOGROUP
     *   XGROUP CREATE -> 建组,但要求 stream 已存在
     * 只有 XGROUP CREATE ... MKSTREAM 能一步把两层都建出来。
     *
     * 为什么走 RedisCallback 发原生命令:本项目锁的是 spring-data-redis 2.6.2,
     * 它的 StreamOperations 只有 createGroup(key, group) 和 createGroup(key, offset, group),
     * 没有 mkStream 那个重载,所以 stream 不存在时会直接失败,只能自己发原始命令。
     */
    private void initConsumerGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<Object>) connection ->
                    connection.execute("XGROUP",
                            "CREATE".getBytes(StandardCharsets.UTF_8),
                            QUEUE_NAME.getBytes(StandardCharsets.UTF_8),
                            GROUP_NAME.getBytes(StandardCharsets.UTF_8),
                            "0".getBytes(StandardCharsets.UTF_8),
                            "MKSTREAM".getBytes(StandardCharsets.UTF_8)));
            log.info("消费者组 {} 初始化完成", GROUP_NAME);
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            // BUSYGROUP = 组已经存在。这是重启应用的常态,绝对不能当错误抛出去,
            // 否则 @PostConstruct 失败会连累整个 Bean 初始化,应用直接起不来
            if (msg.contains("BUSYGROUP")) {
                log.info("消费者组 {} 已存在,跳过创建", GROUP_NAME);
            } else {
                log.error("创建消费者组失败,请检查 Redis 连接", e);
            }
        }
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    //   > 表示只看"从未投递给本组的新消息"
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功:list 为空说明 2 秒内没新消息,回到循环继续等
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    // 2.1 取出消息体,反序列化成订单对象
                    //     排雷:fillBeanWithMap 是把数据"填进"你传进去的那个 bean,再把它返回。
                    //          必须用返回值接住!写成 fillBeanWithMap(value, new VoucherOrder(), true)
                    //          而不赋给任何变量,new 出来的对象填完就没人引用了,后面拿到的永远是 null
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 2.2 一条消息只处理一次(内部有锁 + 事务 + 一人一单兜底)
                    handleVoucherOrder(voucherOrder);
                    // 3.ACK确认:告诉 redis 这条处理完了,从 pending-list 里摘掉
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    // 单个订单处理失败不能拖垮整个线程,记日志接着干下一个
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (running) {

                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    //   偏移量 "0" = 从 pending-list 头部开始,专捞"已投递给本消费者但还没 ACK"的消息
                    //   排雷:这里不要加 block。捞不到就该立刻 break 退出,
                    //        加了 block 只会白等 2 秒才 break,pending-list 本来就该是"捞完就走"
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
                    );
                    // 2.pending-list 空了,说明没有待重试的消息,结束循环
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    // 2.1 取出消息并反序列化(同样要用返回值接住,原因见上面)
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 2.2 重新处理这条失败消息
                    handleVoucherOrder(voucherOrder);
                    // 3.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (running) {
//                try {
//                    // 1.从阻塞队列里取订单:队列空的时候,take() 会在这里挂起等待,不占 CPU
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 2.加锁 + 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    // 关停时 shutdownNow() 会打断 take(),此时直接退出循环
//                    if (!running) {
//                        break;
//                    }
//                    Thread.currentThread().interrupt();
//                } catch (Exception e) {
//                    // 单个订单处理失败不能拖垮整个线程,记日志接着干下一个
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {

        // 排雷:proxy 是在 seckillVoucher() 里赋值的,只在"本次启动后有人下过单"时才不为 null。
        //      如果应用刚起来就从 pending-list 捞到上一轮遗留的消息,proxy 还是 null,下一行就 NPE。
        //      这不是完美方案(消息会进 pending 反复重试),但至少不会把消费线程炸掉。
        if (proxy == null) {
            log.error("proxy 未初始化,无法创建订单,userId={}, voucherId={}",
                    voucherOrder.getUserId(), voucherOrder.getVoucherId());
            return;
        }

        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            log.info("不允许重复下单");
            return;
        }
        try {
           proxy.createVoucherOrder(voucherOrder);
        }finally {
            redisLock.unlock();
        }

    }
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 0.先生成订单id:这个订单号要一路用到落库,整个方法只生成这一个,别生成两次
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本:判库存、判一人一单、扣库存、记录用户,四件事在 redis 里一口气做完
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 2.判断当前是不是0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.4 缓存事务代理:必须在请求线程这里取。
        //     AopContext 底层是 ThreadLocal,后台线程里取会抛 IllegalStateException(Cannot find current proxy)
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 4.返回订单id给前端
        return Result.ok(orderId);
    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 0.先生成订单id:这个订单号要一路用到落库,整个方法只生成这一个,别生成两次
//        long orderId = redisIdWorker.nextId("order");
//        // 1.执行lua脚本:判库存、判一人一单、扣库存、记录用户,四件事在 redis 里一口气做完
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//
//        // 2.判断当前是不是0
//        int r = result.intValue();
//        if (r != 0) {
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        // 3.redis 那边已经扣完库存、记完名单了,这里把订单丢进内存队列,让后台线程慢慢落库
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 3.1 订单id(必须和返回给前端的那个是同一个)
//        voucherOrder.setId(orderId);
//        // 3.2 用户id
//        voucherOrder.setUserId(userId);
//        // 3.3 代金卷id
//        voucherOrder.setVoucherId(voucherId);
//
//        // 3.4 缓存事务代理:必须在请求线程这里取。
//        //     AopContext 底层是 ThreadLocal,后台线程里取会抛 IllegalStateException(Cannot find current proxy)
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 3.5 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 4.返回订单id给前端
//        return Result.ok(orderId);
//    }

    /**
     * 一人一单
     * 超买问题
     * */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 排雷:这里是后台线程,UserHolder 里什么都没有!
        //      它是 ThreadLocal,只存在于 tomcat 处理请求的那个线程。
        //      用户 id 只能从订单对象里拿。
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 一人一单:数据库再查一遍兜底(redis 名单可能丢,数据库才是最终账本)
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 如果有值就直接返回异常给前端
        if (count > 0) {
            log.info("用户已经购买了一次");
            return;
        }
        // 创建订单
        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  // 这个相当于是 set stock = stock - 1
                // 这个相当于是 voucher_id = ? and stock(库存) > 0
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        // 扣减失败说明库存已经被抢光了
        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }

    @PreDestroy
    private void destroy(){
        running = false;
        // 必须用 shutdownNow:take() 是阻塞的,普通 shutdown() 打断不了它,线程会一直卡着
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

//    private void createVoucherOrder(VoucherOrder voucherOrder) {
//        Long userId = voucherOrder.getUserId();
//        Long voucherId = voucherOrder.getVoucherId();
//        // 创建锁对象
//        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
//        // 尝试获取锁
//        boolean isLock = redisLock.tryLock();
//        // 判断
//        if (!isLock) {
//            // 获取锁失败，直接返回失败或者重试
//            log.error("不允许重复下单！");
//            return;
//        }
//
//        try {
//            // 5.1.查询订单
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5.2.判断是否存在
//            if (count > 0) {
//                // 用户已经购买过了
//                log.error("不允许重复下单！");
//                return;
//            }
//
//            // 6.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1") // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
//                    .update();
//            if (!success) {
//                // 扣减失败
//                log.error("库存不足！");
//                return;
//            }
//
//            // 7.创建订单
//            save(voucherOrder);
//        } finally {
//            // 释放锁
//            redisLock.unlock();
//        }
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 根据 voucherId 去查 tb_seckill_voucher（秒杀券表）
//        SeckillVoucher serviceById = seckillVoucherService.getById(voucherId);
//        // 排雷：查不到就直接返回，不然下面 getBeginTime() 会空指针
//        if (serviceById == null) {
//            return Result.fail("优惠券不存在");
//        }
//        // 当前时间只取一次，别在下面反复 LocalDateTime.now()
//        LocalDateTime now = LocalDateTime.now();
//        // 再判断当前时间是不是在活动范围内
//        // `A.isBefore(B)`：判断 A 这个时间，是不是在 B 时间的前面
//        if (now.isBefore(serviceById.getBeginTime())) {
//            return Result.fail("活动还未开始");
//        }
//        // now 不早于 endTime，说明活动已经结束了（这里文案别写成"未开始"）
//        if (!now.isBefore(serviceById.getEndTime())) {
//            return Result.fail("活动已经结束");
//        }
//        // 再查库存 要是没有 就返回一个错误
//        if (serviceById.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        // 注意：这里是「调用」方法，要带括号、要传参
//        // 直接写 return createVoucherOrder; 编译器会把它当变量找，找不到就报错
//        Long userId = UserHolder.getUser().getId();
//
//        // 利用字符串常量池，把每个不同的用户 ID 转换成一把“专属于他自己的、全局唯一的锁”
//        // ，从而实现了既能防止同一个人并发重复下单（精准加锁），又不会影响其他人买券（高并发性能）的高级写法！
//        synchronized(userId.toString().intern()) {
//            // 获取代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }
//    }
}
