package com.alibaba.otter.canal.common.zookeeper.running;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;

/**
 * 针对server的running节点控制
 *
 * @author jianghang 2012-11-22 下午02:59:42
 * @version 1.0.0
 */
public class ServerRunningMonitor extends AbstractCanalLifeCycle {

    private static final Logger logger = LoggerFactory.getLogger(ServerRunningMonitor.class);
    private ZkClientx zkClient;
    private String destination;
    private IZkDataListener dataListener;
    private BooleanMutex mutex = new BooleanMutex(false);
    private volatile boolean release = false;
    // 当前服务节点状态信息
    private ServerRunningData serverData;
    // 当前实际运行的节点状态信息
    private volatile ServerRunningData activeData;
    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
    private int delayTime = 5;
    private ServerRunningListener listener;

    public ServerRunningMonitor(ServerRunningData serverData) {
        this();
        this.serverData = serverData;
    }

    public ServerRunningMonitor() {
        // 创建父节点
        dataListener = new IZkDataListener() {
            /** ！！！目前看来，好像并没有存在修改running节点数据的代码，为什么这个方法不是空实现？ */
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                MDC.put("destination", destination);
                ServerRunningData runningData = JsonUtils.unmarshalFromByte((byte[]) data, ServerRunningData.class);
                if (!isMine(runningData.getAddress())) {
                    mutex.set(false);
                }

                if (!runningData.isActive() && isMine(runningData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
                    releaseRunning();// 彻底释放mainstem
                }

                activeData = runningData;
            }

            /** 当其他canal instance出现异常，临时节点数据被删除时，会自动回调这个方法，此时当前canal instance要顶上去 */
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                MDC.put("destination", destination);
                mutex.set(false);
                if (!release && activeData != null && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    // 尝试自己进入running状态
                    delayExector.schedule(() -> initRunning(), delayTime, TimeUnit.SECONDS);
                }
            }
        };

    }

    public void init() {
        processStart();
    }

    @Override
    public synchronized void start() {
        super.start();
        try {
            // 其内部会调用ServerRunningListener的processStart()方法
            processStart();
            // 若存在zk，以HA方式启动CanalInstance
            if (zkClient != null) {
                // 如果需要尽可能释放instance资源，不需要监听running节点，不然即使stop了这台机器，另一台机器立马会start

                // zk路径为：/otter/canal/destinations/{0}/running
                /*构建临时节点的路径：/otter/canal/destinations/{0}/running，其中占位符{0}会被destination替换。
                在集群模式下，可能会有多个canal server共同处理同一个destination，
                在某一时刻，只能由一个canal server进行处理，处理这个destination的canal server进入running状态，其他canal server进入standby状态。*/
                String path = ZookeeperPathUtils.getDestinationServerRunning(destination);

                /*对destination对应的running节点进行监听，一旦发生了变化，则说明可能其他处理相同destination的canal server可能出现了异常，
                此时需要尝试自己进入running状态。*/
                zkClient.subscribeDataChanges(path, dataListener); // 监听节点数据变化

                initRunning();
            } else {
                processActiveEnter();// 没有zk，直接启动
            }
        } catch (Exception e) {
            logger.error("start failed", e);
            // 没有正常启动，重置一下状态，避免干扰下一次start
            stop();
        }

    }

    public boolean release() {
        if (zkClient != null) {
            releaseRunning(); // 尝试一下release
            return true;
        } else {
            processActiveExit(); // 没有zk，直接退出
            return false;
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();

        if (zkClient != null) {
            String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
            zkClient.unsubscribeDataChanges(path, dataListener);

            releaseRunning(); // 尝试一下release
        } else {
            processActiveExit(); // 没有zk，直接启动
        }
        processStop();
    }

    private void initRunning() {
        if (!isStart()) {
            return;
        }

        // 临时节点zk路径为：/otter/canal/destinations/{0}/running，其中占位符{0}会被destination替换
        String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
        // 序列化，构建临时节点的数据，标记当前destination由哪一个canal server处理
        byte[] bytes = JsonUtils.marshalToByte(serverData);
        try {
            mutex.set(false);
            // 尝试创建临时节点。如果节点已经存在，说明是其他的canal server已经启动了这个canal instance。
            // 此时会抛出ZkNodeExistsException，进入catch代码块。
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);
            activeData = serverData;
            processActiveEnter();// 触发一下事件，启动CanalInstance
            mutex.set(true);
            release = false;
        } catch (ZkNodeExistsException e) {
            //创建节点失败，则根据path从zk中获取当前是哪一个canal server创建了当前canal instance的相关信息。
            //第二个参数true，表示的是，如果这个path不存在，则返回null。
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                //如果的确存在，则将创建该canal instance实例信息存入activeData中。
                activeData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            }
        } catch (ZkNoNodeException e) {
            // 如果/otter/canal/destinations/{0}/节点不存在，进行创建，其中占位符{0}会被destination替换
            zkClient.createPersistent(ZookeeperPathUtils.getDestinationPath(destination), true); // 尝试创建父节点
            initRunning();
        }
    }

    /**
     * 阻塞等待自己成为active，如果自己成为active，立马返回
     *
     * @throws InterruptedException
     */
    public void waitForActive() throws InterruptedException {
        initRunning();
        mutex.get();
    }

    /**
     * 检查当前的状态
     */
    public boolean check() {
        String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
        try {
            byte[] bytes = zkClient.readData(path);
            ServerRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ServerRunningData.class);
            activeData = eventData;// 更新下为最新值
            // 检查下nid是否为自己
            boolean result = isMine(activeData.getAddress());
            if (!result) {
                logger.warn("canal is running in node[{}] , but not in node[{}]",
                        activeData.getAddress(),
                        serverData.getAddress());
            }
            return result;
        } catch (ZkNoNodeException e) {
            logger.warn("canal is not run any in node");
            return false;
        } catch (ZkInterruptedException e) {
            logger.warn("canal check is interrupt");
            Thread.interrupted();// 清除interrupt标记
            return check();
        } catch (ZkException e) {
            logger.warn("canal check is failed");
            return false;
        }
    }

    private boolean releaseRunning() {
        if (check()) {
            release = true;
            String path = ZookeeperPathUtils.getDestinationServerRunning(destination);
            zkClient.delete(path);
            mutex.set(false);
            processActiveExit();
            return true;
        }

        return false;
    }

    // ====================== helper method ======================

    private boolean isMine(String address) {
        return address.equals(serverData.getAddress());
    }

    private void processStart() {
        if (listener != null) {
            try {
                listener.processStart();
            } catch (Exception e) {
                logger.error("processStart failed", e);
            }
        }
    }

    private void processStop() {
        if (listener != null) {
            try {
                listener.processStop();
            } catch (Exception e) {
                logger.error("processStop failed", e);
            }
        }
    }

    private void processActiveEnter() {
        if (listener != null) {
            listener.processActiveEnter();
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            try {
                listener.processActiveExit();
            } catch (Exception e) {
                logger.error("processActiveExit failed", e);
            }
        }
    }

    public void setListener(ServerRunningListener listener) {
        this.listener = listener;
    }

    // ===================== setter / getter =======================

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public void setServerData(ServerRunningData serverData) {
        this.serverData = serverData;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setZkClient(ZkClientx zkClient) {
        this.zkClient = zkClient;
    }

}
