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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

    private IVoucherOrderService proxy;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 底层容器范围 底层的大小 阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // 单线程线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 线程运行开关,volatile 保证多线程可见
    private volatile boolean running = true;

    /**
     *  @PostConstruct 当前类初始化的时候进行加载
     * */
    @PostConstruct
    private void init() {
        // 阻塞队列版不需要 stream 消费者组,只要把后台线程拉起来就行
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    // 1.从阻塞队列里取订单:队列空的时候,take() 会在这里挂起等待,不占 CPU
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.加锁 + 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    // 关停时 shutdownNow() 会打断 take(),此时直接退出循环
                    if (!running) {
                        break;
                    }
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // 单个订单处理失败不能拖垮整个线程,记日志接着干下一个
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {

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
                userId.toString()
        );

        // 2.判断当前是不是0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.redis 那边已经扣完库存、记完名单了,这里把订单丢进内存队列,让后台线程慢慢落库
        VoucherOrder voucherOrder = new VoucherOrder();
        // 3.1 订单id(必须和返回给前端的那个是同一个)
        voucherOrder.setId(orderId);
        // 3.2 用户id
        voucherOrder.setUserId(userId);
        // 3.3 代金卷id
        voucherOrder.setVoucherId(voucherId);

        // 3.4 缓存事务代理:必须在请求线程这里取。
        //     AopContext 底层是 ThreadLocal,后台线程里取会抛 IllegalStateException(Cannot find current proxy)
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3.5 放入阻塞队列
        orderTasks.add(voucherOrder);

        // 4.返回订单id给前端
        return Result.ok(orderId);
    }

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
