import os
import asyncio
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
CORS(app)

# تهيئة الـ SocketIO بنظام الـ threading المتوافق تماماً مع خوادم Render
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',
    ping_timeout=60, 
    ping_interval=25
)

# جلب الـ Token من متغيرات البيئة في Render
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# مخزن الجلسات النشطة في الذاكرة
sessions = {}

# ---------------------------------------------------------------------------
# دالة البث الحي اللحظي (WebSockets Stream) لكل حساب
# ---------------------------------------------------------------------------
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
        
        # حلقة فحص وبث متواصلة كل ثانية واحدة لسرعة البرق
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
            
            initial = float(session_data.get('initial_balance', balance))
            drawdown_percent = abs((pnl / balance) * 100) if pnl < 0 else 0.0
            overall_growth = ((balance - initial) / initial) * 100 if initial > 0 else 0.0

            # 🛑 الردع الصارم: إذا كان الحساب مقفولاً وقام المستخدم بفتح صفقة يدوياً من الميتا
            if session_data.get('is_locked') and len(positions) > 0:
                print(f"🚨 LOCKOUT VIOLATION! Closing illegal positions immediately.")
                for pos in positions:
                    try:
                        await connection.close_position(pos['id'])
                    except:
                        pass
                positions = []
                pnl = 0.0
                drawdown_percent = 0.0

            # 📈 الفحص التلقائي للهدف والخسارة من جهة السيرفر
            if not session_data.get('is_locked'):
                daily_target = session_data.get('daily_target', 500.0)
                max_loss_limit = session_data.get('max_loss_limit', -500.0)
                
                if pnl >= daily_target or pnl <= max_loss_limit:
                    print(f"🎯 Target or Stop reached on server side! Locking down account.")
                    session_data['is_locked'] = True
                    for pos in positions:
                        try:
                            await connection.close_position(pos['id'])
                        except:
                            pass
                    positions = []
                    pnl = 0.0
                    drawdown_percent = 0.0

            # ⚡ ضخ البيانات فوراً إلى تطبيق فلاتر عبر الـ WebSocket Room
            socketio.emit('metrics_update', {
                'session_id': session_id,
                'is_locked': session_data.get('is_locked', False),
                'balance': balance,
                'equity': equity,
                'current_pnl': pnl,
                'drawdown_percent': drawdown_percent,
                'daily_profit': session_data.get('daily_profit', 0.0),
                'overall_growth': overall_growth,
                'open_trades': len(positions)
            }, to=session_id)
            
            await asyncio.sleep(1) # فحص مستمر كل ثانية
            
    except Exception as e:
        print(f"❌ Streaming Error on session {session_id}: {e}")

# دالة لتشغيل البث في خيط معزول وآمن ومحمي من الكراش
def start_async_stream(session_id, account_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(stream_account_metrics(session_id, account_id))
    except Exception as stream_err:
        print(f"⚠️ Stream execution stopped safely: {stream_err}")
    finally:
        loop.close()

# ---------------------------------------------------------------------------
# غرف الـ WebSockets (Rooms) لفرز اتصالات المستخدمين
# ---------------------------------------------------------------------------
@socketio.on('join')
def on_join(data):
    session_id = data.get('session_id')
    if session_id:
        from flask_socketio import join_room
        join_room(session_id)
        print(f"📱 App connected and joined WebSocket room: {session_id}")

# ---------------------------------------------------------------------------
# دالة الاتصال وتهيئة الحساب لأول مرة (HTTP POST)
# ---------------------------------------------------------------------------
@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    try:
        daily_target = float(data.get('daily_target', 500.0))
        max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
    except (ValueError, TypeError):
        daily_target = 500.0
        max_loss_limit = -500.0
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات غير كاملة"}), 400
    
    if not API_TOKEN:
        return jsonify({"status": "error", "message": "METAAPI_TOKEN غير معرف"}), 500
    
    try:
        api = MetaApi(API_TOKEN)
        account = None
        
        # فحص وجود الحساب مسبقاً
        try:
            existing_accounts = asyncio.run(api.metatrader_account_api.get_accounts())
            if existing_accounts and isinstance(existing_accounts, list):
                for acc in existing_accounts:
                    acc_login = acc.get('login') if isinstance(acc, dict) else getattr(acc, 'login', None)
                    if str(acc_login) == str(login):
                        account = acc
                        break
        except Exception as ce:
            print(f"⚠️ Quick check bypass: {ce}")
        
        # إنشاء حساب جديد إن لم يكن موجوداً
        if not account:
            account = asyncio.run(api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': str(password),
                'server': str(server),
                'magic': 999111,
                'keywords': ['trading-guardian']
            }))
        
        account_id = account.id if hasattr(account, 'id') else account.get('id')
        account_state = account.state if hasattr(account, 'state') else account.get('state', '')
        
        if account_state != 'DEPLOYED':
            asyncio.run(account.deploy())
        
        # توليد رقم جلسة فريد وتخزينه
        session_id = f"session_{login}_{int(time.time())}"
        sessions[session_id] = {
            'session_id': session_id,
            'account_id': account_id,
            'login': str(login),
            'server': str(server),
            'active': True,
            'is_locked': False,
            'daily_target': daily_target,
            'max_loss_limit': max_loss_limit,
            'daily_profit': 0.0,
            'balance': 0.0,
            'equity': 0.0
        }
        
        # 🔥 إطلاق أنبوب البث الحي فوراً في الخلفية
        threading.Thread(target=start_async_stream, args=(session_id, account_id), daemon=True).start()
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "account_id": account_id,
            "message": "تم الاتصال وتفعيل بث الحماية اللحظي!"
        }), 201
        
    except Exception as e:
        print(f"Critical Connection Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# باقي دالات التحكم (HTTP POST) لتحديث الأهداف أو الفصل
# ---------------------------------------------------------------------------
@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    data = request.json or {}
    session_id = data.get('session_id')
    if session_id in sessions:
        if 'daily_profit_target' in data:
            sessions[session_id]['daily_target'] = float(data['daily_profit_target'])
        if 'daily_stop_loss' in data:
            sessions[session_id]['max_loss_limit'] = -abs(float(data['daily_stop_loss']))
        return jsonify({"status": "success"}), 200
    return jsonify({"status": "error", "message": "جلسة غير صالحة"}), 401

@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    data = request.json or {}
    session_id = data.get('session_id')
    if session_id in sessions:
        sessions[session_id]['active'] = False
        del sessions[session_id]
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # تشغيل السيرفر الاحترافي الداعم للـ WebSockets والـ Gunicorn معاً
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
