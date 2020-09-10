package com.example.rabbitmqapplication.service

import android.app.Service
import android.content.Intent
import android.os.*
import android.util.Log
import com.gxx.rabbitmqlibrary.RabbitMQUtil
import java.lang.ref.WeakReference

class RoboMqService : Service() {

    private var mClicentMessenger: Messenger? = null;
    private var rabbitReceiveMQUtil: RabbitMQUtil? = null;//用于接收消息
    private var rabbitSendMQUtil: RabbitMQUtil? = null;//用于发送消息
    private var messengerHandler = MessengerHandler(this);
    private var mServiceMessenger = Messenger(messengerHandler)//传递给客户端
    private var mClientMessenger: Messenger? = null;

    companion object {
        var TAG = RoboMqService.javaClass.simpleName;
        const val RABBITMQ_HOSTNAME = "192.168.1.123";
        const val RABBITMQ_PORT = 8888;
        const val RABBITMQ_USERNAME = "zhagnsan";
        const val RABBITMQ_PASSWORD = "123456";

        const val ROBO_MQ_MESSAGE_STATUS_1 = 1;//正常
        const val ROBO_MQ_MESSAGE_STATUS_ERROR = -1;//异常

        const val LOCAL_WHAT_1 = 1;//处理RoboMqService本地的
        const val LOCAL_WHAT_2 = 2;//处理activity的
        const val ROBO_WHAT = "what";
        const val ROBO_SEND_QUEUENAME = "send_queueName"//发送的队列名
        const val ROBO_MQ_MESSAGE = "message"
    }

    override fun onCreate() {
        super.onCreate()
        Log.e(TAG,"创建")
        if (rabbitReceiveMQUtil == null){
            rabbitReceiveMQUtil = RabbitMQUtil(RABBITMQ_HOSTNAME, RABBITMQ_PORT, RABBITMQ_USERNAME, RABBITMQ_PASSWORD);
            //接收到消息后，通知前端activity
            rabbitReceiveMQUtil!!.receiveQueueMessage("这里填写接收的队列名",RabbitMQUtil.ReceiveMessageListener {
                var message = Message.obtain();
                message.arg1 = ROBO_MQ_MESSAGE_STATUS_1;
                message.obj = it;
                message.what = LOCAL_WHAT_1;
                messengerHandler.handleMessage(message);
            },RabbitMQUtil.ErrorMessageListener {
                var message = Message.obtain();
                message.arg1 = ROBO_MQ_MESSAGE_STATUS_ERROR;
                message.obj = it?.message;
                messengerHandler.handleMessage(message);
            })
        }
    }


    class MessengerHandler constructor(roboMqService: RoboMqService) : Handler() {
        var roboMqServiceWeakReference: WeakReference<RoboMqService>? = null;

        init {
            roboMqServiceWeakReference = WeakReference<RoboMqService>(roboMqService);
        }

        override fun handleMessage(msg: Message) {
            if (roboMqServiceWeakReference == null || roboMqServiceWeakReference!!.get() == null) {
                return;
            }

            //得到前端的 Messenger
            if (roboMqServiceWeakReference!!.get()!!.mClicentMessenger == null && msg.replyTo!=null) {
                roboMqServiceWeakReference!!.get()!!.mClicentMessenger = msg.replyTo;
            }

            if (msg.what == LOCAL_WHAT_1) {//当前service类
                try {
                    var replyMsg = Message.obtain();
                    var bundle = Bundle();
                    bundle.putString(ROBO_MQ_MESSAGE, msg.obj as String?)
                    replyMsg.arg1 = msg.arg1;
                    replyMsg.data = bundle;
                    roboMqServiceWeakReference!!.get()!!.mClicentMessenger?.send(replyMsg);
                } catch (e: RemoteException) {
                    e.printStackTrace();
                }
            } else {//由activity 传递参数过来
                //主要是用来发送
                if (roboMqServiceWeakReference!!.get()!!.rabbitSendMQUtil == null && msg.data.getInt(ROBO_WHAT, 0) == LOCAL_WHAT_2) {
                    roboMqServiceWeakReference!!.get()!!.rabbitSendMQUtil = RabbitMQUtil(RABBITMQ_HOSTNAME, RABBITMQ_PORT, RABBITMQ_USERNAME, RABBITMQ_PASSWORD); //设置域名，账号密码
                }
                roboMqServiceWeakReference!!.get()!!.rabbitSendMQUtil!!.sendMessage("你好啊，哈哈哈", msg.data.getString(ROBO_SEND_QUEUENAME),//设置通道名字
                    RabbitMQUtil.SendMessageListener {
                        if (it) {
                            Log.e(TAG, "发送成功啦")
                        }
                    },
                    RabbitMQUtil.ErrorMessageListener {

                    }
                )
            }
        }
    }


    override fun onBind(intent: Intent?): IBinder? {
        if (mServiceMessenger!=null){
            return mServiceMessenger.binder;
        }else{
            return null;
        }
    }

    override fun onDestroy() {
        rabbitReceiveMQUtil?.close();
        rabbitSendMQUtil?.close()
        Log.e(TAG,"销毁了")
        super.onDestroy()
    }
}