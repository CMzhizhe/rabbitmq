package com.gxx.rabbitmqlibrary;

import android.os.SystemClock;
import android.text.TextUtils;

import com.rabbitmq.client.AlreadyClosedException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class RabbitMQUtil {
    private boolean isRunning = true;
    private RabbitMQClient rabbitMQ;
    private ExecutorService executor;


    public RabbitMQUtil(String hostIp, int port, String username, String password) {
        rabbitMQ = new RabbitMQClient(hostIp, port, username, password);
        executor = Executors.newSingleThreadExecutor();  //根据项目需要设置常用线程个数
    }

    /**
     * @param message   发送的消息
     * @param queueName 队列名称
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void sendMessage(final String message, final String queueName, final SendMessageListener sendMessageListener,final ErrorMessageListener errorMessageListener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendQueueMessage(message, queueName);
                    if (sendMessageListener != null) sendMessageListener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (errorMessageListener!=null){
                        errorMessageListener.errorMessage(e);
                    }
                    if (sendMessageListener != null) sendMessageListener.sendMessage(false);
                }
            }
        });
    }

    /**
     * @param message      发送的消息
     * @param exchangeName 交换机名称
     * @param queueName    队列名称
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void sendMessage(final String message, final String exchangeName, final String exchangeType, final String queueName, final String routingKey, final SendMessageListener sendMessageListener,final ErrorMessageListener errorMessageListener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendExchangeNameQueueMessage(exchangeName, exchangeType, message, queueName, routingKey);
                    if (sendMessageListener != null) sendMessageListener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (errorMessageListener!=null){
                        errorMessageListener.errorMessage(e);
                    }
                    if (sendMessageListener != null) sendMessageListener.sendMessage(false);
                }
            }
        });
    }

    /**
     * @param exchangeName 交换机名称
     * @param queueName    队列名称
     * @param message      需要发送的消息
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void sendFanoutTypeMessage(final String exchangeName, final String message, final String queueName, final SendMessageListener sendMessageListener,final ErrorMessageListener errorMessageListener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendFanoutTypeMessage(exchangeName, queueName, message);
                    if (sendMessageListener != null) sendMessageListener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (errorMessageListener!=null){
                        errorMessageListener.errorMessage(e);
                    }
                    if (sendMessageListener != null) sendMessageListener.sendMessage(false);
                }
            }
        });
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
    public void sendDirectTypeMessage(final String exchangeName, final String queueName, final String message, final String routingKey, final SendMessageListener sendMessageListener,final ErrorMessageListener errorMessageListener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rabbitMQ.sendDirectTypeMessage(exchangeName, queueName, message, routingKey);
                    if (sendMessageListener != null) sendMessageListener.sendMessage(true);
                } catch (IOException | TimeoutException | AlreadyClosedException e) {
                    e.printStackTrace();
                    if (errorMessageListener!=null){
                        errorMessageListener.errorMessage(e);
                    }
                    if (sendMessageListener != null) sendMessageListener.sendMessage(false);
                }
            }
        });
    }

    /**
     * @param queueName 队列名称
     * @date 创建时间:2020/9/8 0008
     * @auther gaoxiaoxiong
     * @Descriptiion
     **/
    public void receiveQueueMessage(String queueName, final ReceiveMessageListener listener,final ErrorMessageListener errorMessageListener) {
        String newQueueName = null;
        if (TextUtils.isEmpty(queueName)){
            newQueueName = createDefaultQueueName(queueName);
        }else {
            newQueueName = queueName;
        }
        final String finalNewQueueName = newQueueName;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (isRunning) {
                    try {
                        rabbitMQ.receiveQueueMessage(finalNewQueueName, new RabbitMQClient.ResponseListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }
                        });
                    } catch (IOException | TimeoutException | AlreadyClosedException e) {
                        if (errorMessageListener!=null){
                            errorMessageListener.errorMessage(e);
                        }
                        e.printStackTrace();
                        SystemClock.sleep(5000);
                    }
                }
            }
        });
    }

    public void receiveQueueRoutingKeyMessage(String queueName, final String routingKey, final String exchangeName, final String exchangeType, final ReceiveMessageListener listener,final ErrorMessageListener errorMessageListener) {
        String newQueueName = null;
        if (TextUtils.isEmpty(queueName)){
            newQueueName = createDefaultQueueName(queueName);
        }else {
            newQueueName = queueName;
        }
        final String finalNewQueueName = newQueueName;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (isRunning) {
                    try {
                        rabbitMQ.receiveQueueRoutingKeyMessage(finalNewQueueName, routingKey, exchangeName, exchangeType, new RabbitMQClient.ResponseListener() {
                            @Override
                            public void receive(String message) {
                                if (listener != null) listener.receiveMessage(message);
                            }

                        });
                    } catch (IOException | TimeoutException | AlreadyClosedException e) {
                        if (errorMessageListener!=null){
                            errorMessageListener.errorMessage(e);
                        }
                        e.printStackTrace();
                        SystemClock.sleep(5000);  //等待五秒
                    }
                }
            }
        });
    }

    public String createDefaultQueueName(String routingKey) {
        if (TextUtils.isEmpty(routingKey)){
            routingKey = "";
        }
        return routingKey + "@" + UUID.randomUUID();
    }

    /**
     * 建议：
     * 在application中关闭或者在结束工作时关闭
     */
    public void close() {
        isRunning = false;
        executor.execute(new Runnable() {
            @Override
            public void run() {
                rabbitMQ.close();
                executor.shutdownNow();
            }
        });
    }


    public interface ReceiveMessageListener {
        void receiveMessage(String message);
    }

    public interface SendMessageListener {
        void sendMessage(boolean isSuccess);
    }

    public interface ErrorMessageListener{
        void errorMessage(Exception e);
    }
}
