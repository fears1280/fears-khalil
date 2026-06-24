import os
import asyncio
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit

app = Flask(__name__)
CORS(app)

# التعديل الذهبي لضمان استقرار البث الحي على سيرفرات Render ومنع خروج الكود بـ status 1
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading', # يمنع تعارض الـ Async Loops
    ping_timeout=60, 
    ping_interval=25
)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')
sessions = {}

# دالة لمراقبة حساب ميتاترايدر وضخ البيانات فوراً عبر الـ WebSocket
async def stream_account_metrics(session_id, account_id):
    try:
        api = MetaApi(API_TOKEN)
        account = await api.metatrader_account_api.get_account(account_id)
        
        if account.state != 'DEPLOYED':
            await account.deploy()
            
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        print(f"🚀 Started Live WebSocket Streaming for Session: {session_id}")
        
        # حلقة مستمرة تجلب البيانات بسرعة عالية جداً وتبثها فوراً
        while session_id in sessions:
            if not sessions[session_id].get('active', True):
                break
                
            account_info = await connection.get_account_information()
            positions = await connection.get_positions()
            
            balance = float(account_info.get('balance', 0.0))
            equity = float(account_info.get('equity', 0.0))
            pnl = equity - balance
            
            session_data = sessions[session_id]
            session_data['balance'] = balance
            session_data['equity'] = equity
            
            # آليّة الحظر والقتل الفوري للصفقات المخالفة
            if session_data.get('is_locked') and len(positions) > 0:
                print(f"🚨 Violation Detected via Stream! Closing illegal positions.")
                for pos in positions:
                    try:
                        await connection.close_position(pos['id'])
                    except:
                        pass
                positions = []
                pnl = 0.0

            # الفحص الذكي التلقائي للأهداف داخل السيرفر
            if not session_data.get('is_locked'):
                if pnl >= session_data.get('daily_target', 500.0) or pnl <= session_data.get('max_loss_limit', -500.0):
                    session_data['is_locked'] = True
                    print(f"🎯 Target hit via Live Stream. Locking Session: {session_id}")
                    for pos in positions:
                        try:
                            await connection.close_position(pos['id'])
                        except:
                            pass
                    positions = []
                    pnl = 0.0

            # ⚡ بث البيانات فوراً للهاتف عبر الـ WebSocket بدون انتظار
            socketio.emit('metrics_update', {
                'session_id': session_id,
                'is_locked': session_data.get('is_locked', False),
                'balance': balance,
                'equity': equity,
                'current_pnl': pnl,
                'open_trades': len(positions)
            }, to=session_id)
            
            await asyncio.sleep(1) # فحص وبث متواصل كل ثانية واحدة فقط لسرعة البرق
            
    except Exception as e:
        print(f"❌ Streaming Error on session {session_id}: {e}")

def start_async_stream(session_id, account_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(stream_account_metrics(session_id, account_id))

# غرف الـ WebSockets الخاصة بكل مستخدم
@socketio.on('join')
def on_join(data):
    session_id = data.get('session_id')
    if session_id:
        from flask_socketio import join_room
        join_room(session_id)
        print(f"📱 App joined WebSocket room: {session_id}")

@app.route('/api/connect', methods=['POST'])
def connect():
    # كود الـ connect التقليدي يظل كما هو لتهيئة الجلسة لأول مرة وعمل الـ Deploy
    # (نفس دالة الـ connect الأخيرة التي نستخدمها لتوليد الـ session_id)
    # بمجرد توليد الجلسة، نقوم بتشغيل خيط البث الحي:
    # threading.Thread(target=start_async_stream, args=(session_id, account_id)).start()
    pass

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # تشغيل السيرفر باستخدام SocketIO بدلاً من app.run التقليدية
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
