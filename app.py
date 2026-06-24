import os
import json
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
from metaapi_cloud_sdk import MetaApi # استيراد الحزمة فقط هنا دون تشغيلها عالمياً

app = Flask(__name__)
CORS(app)

# ==================== إعدادات النظام ====================
API_TOKEN = os.getenv("METAAPI_TOKEN")
if not API_TOKEN:
    raise ValueError("METAAPI_TOKEN غير موجود في متغيرات البيئة!")

# ==================== إدارة الجلسات ====================
SESSIONS_FILE = "guardian_sessions.json"
SHARED_SESSION = {}
SESSION_LOCK = threading.Lock()

def load_sessions():
    global SHARED_SESSION
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, 'r') as f:
                SHARED_SESSION = json.load(f)
        except Exception as e:
            print(f"Error loading sessions: {e}")
            SHARED_SESSION = {}

def save_sessions():
    with SESSION_LOCK:
        try:
            with open(SESSIONS_FILE, 'w') as f:
                json.dump(SHARED_SESSION, f, indent=2)
        except Exception as e:
            print(f"Error saving sessions to disk: {e}")

def get_session(session_id):
    return SHARED_SESSION.get(session_id, None)

def update_session(session_id, data):
    with SESSION_LOCK:
        SHARED_SESSION[session_id] = data
        save_sessions()

# ==================== دالة Async آمنة ====================
def run_async(coro):
    """تشغيل coroutine في loop جديدة دائماً لتجنب تعارض Gunicorn"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.close()
        except:
            pass

# ==================== Background Risk Monitor ====================
class RiskMonitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
        self.check_interval = 5
        
    def run(self):
        print("Risk Monitor started...")
        while self.running:
            try:
                self.check_all_accounts()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"Risk Monitor error: {e}")
                time.sleep(self.check_interval)
                
    def check_all_accounts(self):
        with SESSION_LOCK:
            sessions = list(SHARED_SESSION.items())
            
        for session_id, session_data in sessions:
            if session_data.get('status') != 'connected' or session_data.get('is_locked'):
                continue
                
            daily_loss = session_data.get('daily_loss', 0)
            daily_profit = session_data.get('daily_profit', 0)
            max_loss_limit = session_data.get('max_loss_limit', -500)
            daily_target = session_data.get('daily_target', 500)
            
            if daily_loss <= max_loss_limit:
                print(f"🚨 Max loss reached for {session_id}")
                self.emergency_lockdown(session_id, "reached_max_loss")
                
            elif daily_profit >= daily_target:
                print(f"🎯 Daily target reached for {session_id}")
                self.emergency_lockdown(session_id, "reached_daily_target")
                    
    def emergency_lockdown(self, session_id, reason):
        session = get_session(session_id)
        if not session:
            return
            
        try:
            account_id = session.get('account_id')
            run_async(self.close_all_positions(account_id))
            
            session['is_locked'] = True
            session['locked_at'] = datetime.now().isoformat()
            session['unlock_at'] = (datetime.now() + timedelta(hours=2)).isoformat()
            session['lockdown_reason'] = reason
            
            update_session(session_id, session)
            print(f"🔒 Account {session_id} locked successfully. Reason: {reason}")
            
            async def stop_acc():
                api = MetaApi(API_TOKEN) # تأمين الـ Event Loop داخلياً
                acc = await api.metatrader_account_api.get_account(account_id)
                await acc.undeploy()
            try:
                run_async(stop_acc())
            except:
                pass
                
        except Exception as e:
            print(f"Lockdown error for session {session_id}: {e}")
            
    async def close_all_positions(self, account_id):
        try:
            api = MetaApi(API_TOKEN) # تأمين الـ Event Loop داخلياً
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
            
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            positions = await connection.get_positions()
            for pos in positions:
                action = 'SELL' if pos['type'] == 'POSITION_TYPE_BUY' else 'BUY'
                try:
                    await connection.create_market_order(
                        pos['symbol'], 
                        action, 
                        pos['volume'], 
                        {'positionId': pos['id']}
                    )
                    print(f"✅ Closed position {pos['id']} for {pos['symbol']}")
                except Exception as ce:
                    print(f"💥 Failed to close individual position {pos['id']}: {ce}")
                    
        except Exception as e:
            print(f"Close positions connection error: {e}")

# ==================== API Endpoints ====================

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    daily_target = float(data.get('daily_target', 500.0))
    max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات غير كاملة"}), 400
    
    try:
        async def register_account():
            api = MetaApi(API_TOKEN)
            
            # 1. جلب كل الحسابات الموجودة حالياً على MetaApi للتحقق
            existing_accounts = await api.metatrader_account_api.get_accounts()
            account = None
            
            # البحث عن الحساب برقم الـ Login لمنع التكرار العشوائي
            for acc in existing_accounts:
                acc_login = getattr(acc, 'login', None) or (acc.get('login') if isinstance(acc, dict) else None)
                if str(acc_login) == str(login):
                    account = acc
                    print(f"♻️ Found existing MetaApi account for login: {login}")
                    break
            
            # 2. إذا لم نجد حساباً مسجلاً مسبقاً، نقوم بإنشاء واحد جديد
            if not account:
                print(f"✨ Creating new MetaApi account for login: {login}")
                account = await api.metatrader_account_api.create_account({
                    'name': f'Guardian_{login}',
                    'type': 'cloud',
                    'platform': 'mt5',
                    'login': str(login),
                    'password': password,
                    'server': server,
                    'magic': 999111,
                    'keywords': ['trading-guardian']
                })
            
            # 3. التأكد من أن الحساب مفعّل (Deployed) على السيرفر
            if account.state != 'DEPLOYED':
                await account.deploy()
            
            await account.wait_connected(timeout_in_seconds=30)
            
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            account_info = await connection.get_account_information()
            initial_balance = float(account_info.get('balance', 0.0))
            
            # إنشاء جلسة فريدة للتطبيق
            session_id = f"session_{login}_{int(time.time())}"
            session_data = {
                'session_id': session_id,
                'account_id': account.id,
                'login': login,
                'server': server,
                'status': 'connected',
                'connected_at': datetime.now().isoformat(),
                'daily_target': daily_target,
                'max_loss_limit': max_loss_limit,
                'is_locked': False,
                'daily_profit': 0.0,
                'daily_loss': 0.0,
                'balance': initial_balance,
                'equity': initial_balance,
                'drawdown': 0.0,
                'positions_count': 0,
                'max_trades_per_day': 4,
                'trades_today': 0,
                'initial_balance': initial_balance
            }
            
            update_session(session_id, session_data)
            
            return {
                "status": "success",
                "session_id": session_id,
                "account_id": account.id,
                "message": "تم الاتصال بنجاح وتأمين الحساب من التكرار!"
            }
        
        result = run_async(register_account())
        return jsonify(result), 201 if result.get('status') == 'success' else 500
        
    except Exception as e:
        print(f"Connection API Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    session_id = request.args.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    if session.get('status') != 'connected':
        return jsonify({"status": "error", "message": "الحساب غير متصل"}), 400
    
    if session.get('is_locked'):
        unlock_time_str = session.get('unlock_at')
        try:
            unlock_time = datetime.fromisoformat(unlock_time_str)
            if datetime.utcnow() < unlock_time:
                return jsonify({
                    "status": "locked",
                    "reason": session.get('lockdown_reason'),
                    "locked_until": session.get('unlock_at'),
                    "message": "الحساب مقفول حالياً لحمايتك من السوق"
                }), 200
            else:
                session['is_locked'] = False
                update_session(session_id, session)
        except Exception as e:
            print(f"Error checking unlock time: {e}")
    
    try:
        async def fetch_data():
            api = MetaApi(API_TOKEN) # تأمين الـ Event Loop داخلياً
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            
            if account.state != 'DEPLOYED':
                await account.deploy()
            
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            state = await connection.get_account_information()
            positions = await connection.get_positions()
            
            balance = float(state.get('balance', 0.0)) if isinstance(state, dict) else float(state.balance)
            equity = float(state.get('equity', 0.0)) if isinstance(state, dict) else float(state.equity)
            
            current_pnl = equity - balance
            drawdown_percent = ((balance - equity) / balance * 100) if balance > equity else 0.0
            
            if session.get('initial_balance', 0) == 0 and balance > 0:
                session['initial_balance'] = balance
            
            initial_balance = session.get('initial_balance', balance)
            overall_growth = ((balance - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0.0
            
            session.update({
                'balance': balance,
                'equity': equity,
                'daily_profit': float(current_pnl) if current_pnl > 0 else 0.0,
                'daily_loss': float(current_pnl) if current_pnl < 0 else 0.0,
                'drawdown': float(abs(drawdown_percent)),
                'positions_count': len(positions),
                'last_update': datetime.now().isoformat()
            })
            update_session(session_id, session)
            
            return {
                "status": "success",
                "data": {
                    "balance": balance,
                    "equity": equity,
                    "current_pnl": float(current_pnl),
                    "drawdown_percent": float(abs(drawdown_percent)),
                    "daily_profit": float(current_pnl) if current_pnl > 0 else 0.0,
                    "daily_loss": float(current_pnl) if current_pnl < 0 else 0.0,
                    "overall_growth": float(overall_growth),
                    "open_trades": len(positions),
                    "is_locked": session.get('is_locked', False),
                    "last_update": datetime.now().isoformat()
                }
            }
        
        result = run_async(fetch_data())
        return jsonify(result), 200 if result.get('status') == 'success' else 500
        
    except Exception as e:
        print(f"Error fetching data from MT5: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    try:
        async def undeploy_account():
            api = MetaApi(API_TOKEN) # تأمين الـ Event Loop داخلياً
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            await account.undeploy()
            
            session['status'] = 'disconnected'
            update_session(session_id, session)
            return {"status": "success", "message": "تم قطع الاتصال وحذف الـ Deployment بنجاح"}
        
        result = run_async(undeploy_account())
        return jsonify(result), 200 if result.get('status') == 'success' else 500
        
    except Exception as e:
        print(f"Disconnect Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    data = request.json or {}
    session_id = data.get('session_id')
    reason = data.get('reason', 'manual')
    lockout_hours = int(data.get('lockout_hours', 2))
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    try:
        account_id = session.get('account_id')
        
        async def close_positions():
            api = MetaApi(API_TOKEN) # تأمين الـ Event Loop داخلياً
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
            
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            positions = await connection.get_positions()
            for pos in positions:
                action = 'SELL' if pos['type'] == 'POSITION_TYPE_BUY' else 'BUY'
                await connection.create_market_order(pos['symbol'], action, pos['volume'], {'positionId': pos['id']})
            return True
        
        run_async(close_positions())
        
        session['is_locked'] = True
        session['locked_at'] = datetime.now().isoformat()
        session['unlock_at'] = (datetime.now() + timedelta(hours=lockout_hours)).isoformat()
        session['lockdown_reason'] = reason
        update_session(session_id, session)
        
        return jsonify({
            "status": "success",
            "message": f"تم الإغلاق الطارئ بنجاح وتفعيل الحظر بطلب: {reason}"
        }), 200
        
    except Exception as e:
        print(f"Emergency API Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    if 'daily_profit_target' in data:
        session['daily_target'] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        session['max_loss_limit'] = -abs(float(data['daily_stop_loss']))
    if 'lockout_hours' in data:
        session['lockout_hours'] = int(data['lockout_hours'])
    
    update_session(session_id, session)
    
    return jsonify({
        "status": "success",
        "message": "تم تحديث إعدادات الأهداف والمخاطر بنجاح"
    }), 200


@app.route('/api/unlock/<session_id>', methods=['POST'])
def manual_unlock(session_id):
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    session['is_locked'] = False
    session['unlock_at'] = datetime.now().isoformat()
    update_session(session_id, session)
    
    return jsonify({"status": "success", "message": "تم فك الحظر اليدوي بنجاح"}), 200


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "السيرفر يعمل بكفاءة",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(SHARED_SESSION)
    }), 200


# ==================== تهيئة النظام ====================
load_sessions()

risk_monitor = RiskMonitor()
risk_monitor.start()

if __name__ == '__main__':
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
