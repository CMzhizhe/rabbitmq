package com.gxx.rabbitmqlibrary;

import android.text.TextUtils;
import android.util.Log;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitMQClient {
    private final String TAG = "RabbitMQ";
    private final String FLAG_SEND = "send";
    private final String FLAG_RECEIVE = "receive";

    private final ConnectionFactory factory;
    private Connection connection;
    private Map<String, Channel> channelMap = new HashMap<>();

    public static final String EXCHANGETYPE_FANOUT = "fanout";   //不用匹配路由，发送给所有绑定转换器的队列
    public static final String EXCHANGETYPE_DIRECT = "direct";  //匹配路由一致，才发送给绑定转换器队列
    public static final String EXCHANGETYPE_TOPIC = "topic";  // 通配符* 和 # 匹配路由一致，才发送给绑定转换器队列


    public RabbitMQClient(String hostIp, int port, String username, String password) {
        factory = new ConnectionFactory();
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setHost(hostIp);
        factory.setPort(port);
        factory.setVirtualHost("/");
        factory.setConnectionTimeout(15 * 1000);         //连接时间设置为10秒
        factory.setAutomaticRecoveryEnabled(true);   //恢复连接，通道
        factory.setTopologyRecoveryEnabled(true);    //恢复通道中 转换器，队列，绑定关系等
        factory.setNetworkRecoveryInterval(5 * 1000);    //恢复连接间隔，默认5秒
    }


    /**
     * @param message   需要发送的消息
     * @param queueName 管道名称
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void sendQueueMessage(String message, String queueName) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (!channelMap.containsKey(FLAG_SEND + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channelMap.put(FLAG_SEND + queueName, channel);
        }
        //空名字的交换机，需要设置routingKey，此时会将routingKey 作为 队列名使用
        channelMap.get(FLAG_SEND + queueName).basicPublish("", queueName, null, message.getBytes());
    }


    /**
     * @param exchangeName 交换机名称
     * @param message      需要发送的消息
     * @param queueName    队列名称
     * @param routingKey   路由规则
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion 发送 exchangeType direct 类型的信息
     **/
    public void sendDirectTypeMessage(String exchangeName, String message, String queueName, String routingKey) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (!channelMap.containsKey(FLAG_SEND + exchangeName + EXCHANGETYPE_DIRECT + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channel.exchangeDeclare(exchangeName, EXCHANGETYPE_DIRECT);
            channelMap.put(FLAG_SEND + exchangeName + EXCHANGETYPE_DIRECT + queueName, channel);
        }
        channelMap.get(FLAG_SEND + exchangeName + EXCHANGETYPE_DIRECT + queueName).basicPublish(exchangeName, routingKey, null, message.getBytes());
    }

    /**
     * @param exchangeName 交换机名称
     * @param queueName    队列名称
     * @param message      发送的消息
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion 发送 exchangeType fanout 类型的信息
     **/
    public void sendFanoutTypeMessage(String exchangeName, String queueName, String message) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (!channelMap.containsKey(FLAG_SEND + exchangeName + EXCHANGETYPE_FANOUT + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channel.exchangeDeclare(exchangeName, EXCHANGETYPE_FANOUT);
            channelMap.put(FLAG_SEND + exchangeName + EXCHANGETYPE_FANOUT + queueName, channel);
        }
        channelMap.get(FLAG_SEND + exchangeName + EXCHANGETYPE_FANOUT + queueName).basicPublish(exchangeName, "", null, message.getBytes());
    }

    /**
     * @param exchangeName 交换机名称
     * @param exchangeType 模式
     * @param queueName    队列名称
     * @param message      需要发送的消息
     * @param routingKey   路由规则
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void sendExchangeNameQueueMessage(String exchangeName, String exchangeType, String message, String queueName, String routingKey) throws IOException, TimeoutException, AlreadyClosedException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (!channelMap.containsKey(FLAG_SEND + exchangeName + exchangeType + queueName)) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channel.exchangeDeclare(exchangeName, exchangeType);
            channelMap.put(FLAG_SEND + exchangeName + exchangeType + queueName, channel);
        }
        if (exchangeType.equals(EXCHANGETYPE_FANOUT)) {
            channelMap.get(FLAG_SEND + exchangeName + exchangeType + queueName).basicPublish(exchangeName, "", null, message.getBytes());
        } else if (exchangeType.equals(EXCHANGETYPE_DIRECT)) {
            channelMap.get(FLAG_SEND + exchangeName + exchangeType + queueName).basicPublish(exchangeName, routingKey, null, message.getBytes());
        } else if (exchangeType.equals(EXCHANGETYPE_TOPIC)) {
            channelMap.get(FLAG_SEND + exchangeName + exchangeType + queueName).basicPublish(exchangeName, routingKey, null, message.getBytes());
        }
    }


    /**
     * @param queueName 队列名称
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void receiveQueueMessage(final String queueName, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {
        receiveQueueRoutingKeyMessage(queueName, "", "", "", listener);
    }


    /**
     * @param queueName    队列名称
     * @param routingKey   路由规则
     * @param exchangeName 交换机名称
     * @param exchangeType 交换机类型
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void receiveQueueRoutingKeyMessage(String queueName, final String routingKey, String exchangeName, String exchangeType, final ResponseListener listener)
            throws IOException, TimeoutException, AlreadyClosedException {

        if (exchangeType.equals(EXCHANGETYPE_DIRECT) || exchangeType.equals(EXCHANGETYPE_TOPIC)) {
            if (TextUtils.isEmpty(routingKey)) {
                throw new NullPointerException("路由规则不能为空");
            }
        }

        if (!TextUtils.isEmpty(routingKey)) {
            if (TextUtils.isEmpty(exchangeName)) {
                throw new NullPointerException("交换机名称不能为空");
            }
        }

        if (!channelMap.containsKey(FLAG_RECEIVE + routingKey + queueName)) {
            if (connection == null || !connection.isOpen()) {
                connection = factory.newConnection();
            }

            final Channel channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            //绑定转换器，使用路由筛选消息
            if (!TextUtils.isEmpty(routingKey)) {
                channel.exchangeDeclare(exchangeName, exchangeType);
                channel.queueBind(queueName, exchangeName, routingKey);  //设置绑定
            }
            //监听队列
            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    if (listener != null) {
                        listener.receive(message);
                    }
                    channel.basicAck(envelope.getDeliveryTag(), false);  //消息应答
                }
            });
            channelMap.put(FLAG_RECEIVE + routingKey + queueName, channel);
            Log.e(TAG,"已经连接上了，队列名称：" + queueName);
        }
    }


    /**
     * 关闭所有资源
     */
    public void close() {
        for (Channel next : channelMap.values()) {
            if (next != null && next.isOpen()) {
                try {
                    next.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }
        channelMap.clear();
        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public interface ResponseListener {
        void receive(String message);
    }
}
