package com.example.rabbitmqapplication

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.*
import android.util.Log
import android.view.View
import android.widget.Button
import androidx.appcompat.app.AppCompatActivity
import com.example.rabbitmqapplication.service.RoboMqService
import com.example.rabbitmqapplication.service.RoboMqService.Companion.ROBO_MQ_MESSAGE
import com.example.rabbitmqapplication.service.RoboMqService.Companion.ROBO_SEND_QUEUENAME

class MainActivity : AppCompatActivity() {

    companion object{
        var TAG = MainActivity.javaClass.simpleName;
    }

    private var mServiceMessenger: Messenger? = null;
    /**
     * @date 创建时间: 2020/9/10
     * @auther gaoxiaoxiong
     * @description 客户端Messenger对象
     **/
    private var mClientMessenger = Messenger(MessengerHandler())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        bindMessengerService();
        var buttonSendMessage = findViewById<Button>(R.id.bt_main_sendmessage)
        buttonSendMessage.setOnClickListener(View.OnClickListener {
                //主动发送消息
            try {
                var message = Message.obtain();
                var bundle = Bundle();
                bundle.putInt(RoboMqService.ROBO_WHAT,2);
                bundle.putString(ROBO_SEND_QUEUENAME,"@91114a89-d4d7-495f-b703-3504f9855722");//队列名
                message.data = bundle;
                mServiceMessenger?.send(message)
            } catch (e: RemoteException) {
                e?.printStackTrace();
            }
        })

    }

    private var mMessengerConnection = object : ServiceConnection {
        override fun onServiceDisconnected(name: ComponentName?) {
            mServiceMessenger = null;
        }

        override fun onServiceConnected(name: ComponentName?, iBinder: IBinder?) {
            try {
                var message = Message.obtain();
                if (mServiceMessenger == null){
                    mServiceMessenger = Messenger(iBinder);//这里是拿到 RoboMqService的Messenger，&& 这里面封装了 RoboMqService 的Handler
                }
                //将客户端的Msssenger对象传递给服务器
                message.replyTo = mClientMessenger;
                var bundle = Bundle();
                bundle.putInt(RoboMqService.ROBO_WHAT,2);
                bundle.putString(ROBO_SEND_QUEUENAME,"@91114a89-d4d7-495f-b703-3504f9855722");//队列名
                message.data = bundle;
                mServiceMessenger?.send(message)
            } catch (e: RemoteException) {
                e?.printStackTrace();
            }
        }

    }


    private fun bindMessengerService() {
        var intent = Intent(this, RoboMqService.javaClass);
        bindService(intent,mMessengerConnection, Context.BIND_AUTO_CREATE);
    }

    class MessengerHandler : Handler() {
        override fun handleMessage(msg: Message) {
            if (msg.data!=null && msg.data.getString(ROBO_MQ_MESSAGE)!=null){
                Log.e(TAG,"数据："+ msg.data.getString(ROBO_MQ_MESSAGE))
            }
        }
    }

    override fun onDestroy() {
        if (mMessengerConnection!=null){
            unbindService(mMessengerConnection!!);
        }
        super.onDestroy()
    }
}