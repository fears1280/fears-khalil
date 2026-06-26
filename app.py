import sys
import flask
import werkzeug.serving
from werkzeug.local import LocalStack

# 1️⃣ الخدعة الأولى: حل مشكلة الـ Stack المحذوف في Flask 3
flask._request_ctx_stack = LocalStack()

# 2️⃣ الخدعة الثانية: تزوير الدالة المحذوفة في Werkzeug 3 لمنع كراش الـ Import
werkzeug.serving.run_with_reloader = lambda *args, **kwargs: None

# ---------------------------------------------------------------------------
# استيراد المكتبات بشكل آمن ومستقر
# ---------------------------------------------------------------------------

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

# فحص التوكن للتأكد من وصوله للسيرفر
if not API_TOKEN:
    print("❌ خطأ فادح: السيرفر لا يرى متغير METAAPI_TOKEN! تأكد من إضافته في Render.")
else:
    print(f"✅ تم قراءة التوكن بنجاح! يبدأ بـ: ({API_TOKEN[:5]}...)")

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

            # الردع الصارم لإغلاق الصفقات غير القانونية في حال قفل الحساب
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

            # الفحص التلقائي للأهداف والخسائر
            if not session_data.get('is_locked'):
                daily_target = session_data.get('daily_target', 500.0)
                max_loss_limit = session_data.get('max_loss_limit', -500.0)
                
                if pnl >= daily_target or pnl <= max_loss_limit:
                    print(f"🎯 Target or Stop reached! Locking down account.")
                    session_data['is_locked'] = True
                    for pos in positions:
                        try:
                            await connection.close_position(pos['id'])
                        except:
                            pass
                    positions = []
                    pnl = 0.0
                    drawdown_percent = 0.0

            # ضخ البيانات إلى تطبيق فلاتر
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
            
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"❌ Streaming Error on session {session_id}: {e}")

def start_async_stream(session_id, account_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(stream_account_metrics(session_id, account_id))
    except Exception as stream_err:
        print(f"⚠️ Stream execution stopped safely: {stream_err}")
    finally:
        loop.close()

@socketio.on('join')
def on_join(data):
    session_id = data.get('session_id')
    if session_id:
        from flask_socketio import join_room
        join_room(session_id)
        print(f"📱 App joined WebSocket room: {session_id}")

# ---------------------------------------------------------------------------
# دالة معالجة الاتصال بـ MetaApi (محدثة لتصحيح أنواع البيانات)
# ---------------------------------------------------------------------------
async def _handle_metaapi_connection(api, login_int, password, server):
    account = None
    
    # 1️⃣ محاولة فحص إذا كان الحساب مضافاً مسبقاً
    try:
        existing_accounts = await api.metatrader_account_api.get_accounts()
        if existing_accounts and isinstance(existing_accounts, list):
            for acc in existing_accounts:
                acc_login = acc.login if hasattr(acc, 'login') else acc.get('login', None)
                if str(acc_login) == str(login_int):
                    account = acc
                    break
    except Exception as ce:
        print(f"⚠️ Quick check bypass: {ce}")
    
    # 2️⃣ إنشاء حساب جديد (تم تحويل الـ login إلى int إجباري لمنع رفض الـ SDK للطلب)
    if not account:
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login_int}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': int(login_int),  # رقم صحيح إلزامي هنا لـ MetaApi
            'password': str(password),
            'server': str(server),
            'magic': 999111,
            'keywords': ['trading-guardian']
        })
    
    # جلب المعرف وحالة الحساب الحالية بشكل آمن
    account_id = account.id if hasattr(account, 'id') else account.get('id')
    account_state = account.state if hasattr(account, 'state') else account.get('state', '')
    
    # 3️⃣ تفعيل الحساب وعمل Deploy له إذا لم يكن مفعلاً
    if account_state != 'DEPLOYED':
        if hasattr(account, 'deploy'):
            await account.deploy()
        else:
            fresh_account = await api.metatrader_account_api.get_account(account_id)
            await fresh_account.deploy()
        
    return account_id

# ---------------------------------------------------------------------------
# دالة الـ API الأساسية للتسجيل (HTTP POST)
# ---------------------------------------------------------------------------
@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    # التحقق من صحة رقم الـ login وتحويله لرقم صحيح لمنع كراش السيرفر الداخلي
    try:
        login_int = int(login)
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "رقم الـ Login غير صحيح، يجب أن يتكون من أرقام فقط"}), 400
    
    try:
        daily_target = float(data.get('daily_target', 500.0))
        max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
    except (ValueError, TypeError):
        daily_target = 500.0
        max_loss_limit = -500.0
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات غير كاملة، يرجى ملء جميع الحقول"}), 400
    
    if not API_TOKEN:
        return jsonify({"status": "error", "message": "METAAPI_TOKEN غير معرف في لوحة تحكم Render"}), 500
    
    try:
        # أ) إنشاء الـ Event Loop أولاً وتفعيله في الخيط الحالي
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # ب) تهيئة كائن الـ MetaApi *داخل* بيئة الـ Loop النشط لربط الـ sessions بشكل سليم
        api = MetaApi(API_TOKEN)
        
        # ج) تنفيذ المهام في تدفق برمي واحد متكامل وبدون انقطاع
        account_id = loop.run_until_complete(
            _handle_metaapi_connection(api, login_int, password, server)
        )
        loop.close()  # إغلاق آمن للـ Loop بعد النجاح
        
        # توليد رقم جلسة فريد وتخزينه في الذاكرة
        session_id = f"session_{login_int}_{int(time.time())}"
        sessions[session_id] = {
            'session_id': session_id,
            'account_id': account_id,
            'login': str(login_int),
            'server': str(server),
            'active': True,
            'is_locked': False,
            'daily_target': daily_target,
            'max_loss_limit': max_loss_limit,
            'daily_profit': 0.0,
            'balance': 0.0,
            'equity': 0.0
        }
        
        # إطلاق أنبوب البث الحي اللحظي في الخلفية
        threading.Thread(target=start_async_stream, args=(session_id, account_id), daemon=True).start()
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "account_id": account_id,
            "message": "تم الاتصال وتفعيل بث الحماية اللحظي بنجاح!"
        }), 201
        
    except Exception as e:
        # طباعة الخطأ الفعلي كاملاً في سيرفر Render لمعرفته بدقة
        print(f"❌ Critical Connection Error: {str(e)}")
        return jsonify({"status": "error", "message": f"خطأ في الاتصال بـ MetaApi: {str(e)}"}), 500

# ---------------------------------------------------------------------------
# باقي دالات التحكم للتحديث والفصل
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
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
