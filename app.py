from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
import time
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path

app = Flask(__name__)
CORS(app)  # تفعيل CORS للسماح بطلبات Flutter

# ==================== إعدادات النظام ====================
API_TOKEN = os.getenv("METAAPI_TOKEN")
if not API_TOKEN:
    raise ValueError("❌ METAAPI_TOKEN غير موجود في متغيرات البيئة!")

api = MetaApi(API_TOKEN)

# ==================== إدارة الجلسات ====================
SESSIONS_FILE = "guardian_sessions.json"
SHARED_SESSION = {}  # ذاكرة الرام الحية
SESSION_LOCK = threading.Lock()  # منع تضارب البيانات

def load_sessions():
    """تحميل الجلسات المحفوظة من الملف"""
    global SHARED_SESSION
    if os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, 'r') as f:
                SHARED_SESSION = json.load(f)
        except:
            SHARED_SESSION = {}
    return SHARED_SESSION

def save_sessions():
    """حفظ الجلسات في الملف"""
    with SESSION_LOCK:
        with open(SESSIONS_FILE, 'w') as f:
            json.dump(SHARED_SESSION, f, indent=2)

def get_session(session_id):
    """استخراج بيانات جلسة معينة"""
    return SHARED_SESSION.get(session_id, None)

def update_session(session_id, data):
    """تحديث بيانات الجلسة"""
    with SESSION_LOCK:
        SHARED_SESSION[session_id] = data
        save_sessions()

# ==================== دالة Async آمنة ====================
def run_async(coro):
    """تشغيل عمليات async بشكل آمن في أي سياق (Thread/Loop)"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # إذا كانت loop تعمل في thread الحالي، ننشئ loop جديدة في thread منفصل
            def _run_in_new_loop(c):
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(c)
            
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(_run_in_new_loop, coro).result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # لا توجد loop في هذا thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)

# ==================== Background Risk Monitor ====================
class RiskMonitor(threading.Thread):
    """خيط الحماية الذي يراقب الحسابات بشكل مستمر"""
    
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
        self.check_interval = 4  # كل 4 ثوانٍ
        
    def run(self):
        """حلقة المراقبة المستمرة"""
        print("🛡️  Risk Monitor بدأ يعمل...")
        while self.running:
            try:
                self.check_all_accounts()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"⚠️  خطأ في Risk Monitor: {e}")
                
    def check_all_accounts(self):
        """فحص جميع الحسابات المتصلة"""
        with SESSION_LOCK:
            sessions = list(SHARED_SESSION.items())
            
        for session_id, session_data in sessions:
            if session_data.get('status') != 'connected':
                continue
                
            # تحقق من الحد الأقصى للخسارة
            daily_loss = session_data.get('daily_loss', 0)
            daily_profit = session_data.get('daily_profit', 0)
            max_loss_limit = session_data.get('max_loss_limit', -500)
            daily_target = session_data.get('daily_target', 500)
            
            # فعّل الحظر إذا تحققت شروط الإيقاف
            if daily_loss <= max_loss_limit:
                print(f"🚨 تحذير: الحساب {session_id} ضرب الحد الأقصى للخسارة!")
                self.emergency_lockdown(session_id, "reached_max_loss")
                
            elif daily_profit >= daily_target:
                print(f"🎯 تحذير: الحساب {session_id} حقق الهدف اليومي!")
                self.emergency_lockdown(session_id, "reached_daily_target")
                    
    def emergency_lockdown(self, session_id, reason):
        """إغلاق جميع الصفقات وحظر الحساب"""
        session = get_session(session_id)
        if not session:
            return
            
        try:
            account_id = session.get('account_id')
            
            # إغلاق جميع الصفقات في thread منفصل بـ loop جديدة
            def _close_positions():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(self.close_all_positions(account_id))
            
            with concurrent.futures.ThreadPoolExecutor() as pool:
                pool.submit(_close_positions).result(timeout=30)
            
            # تفعيل الحظر
            session['is_locked'] = True
            session['locked_at'] = datetime.now().isoformat()
            session['unlock_at'] = (datetime.now() + timedelta(hours=2)).isoformat()
            session['lockdown_reason'] = reason
            
            update_session(session_id, session)
            print(f"🔒 تم حظر الحساب {session_id} لسبب: {reason}")
            
        except Exception as e:
            print(f"❌ خطأ في الحظر الإجباري: {e}")
            
    async def close_all_positions(self, account_id):
        """إغلاق جميع الصفقات المفتوحة"""
        try:
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
            
            # إغلاق جميع الصفقات
            positions = await account.get_positions()
            for position in positions:
                await account.close_position_by_symbol(position.symbol)
                
            print(f"✅ تم إغلاق جميع الصفقات للحساب {account_id}")
        except Exception as e:
            print(f"❌ خطأ في إغلاق الصفقات: {e}")

# ==================== API Endpoints ====================

@app.route('/api/connect', methods=['POST'])
def connect():
    """ربط حساب MetaTrader 5 جديد - يتوافق مع Flutter"""
    data = request.json
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    daily_target = data.get('daily_target', 500)
    max_loss_limit = data.get('max_loss_limit', -500)
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات غير كاملة"}), 400
    
    async def register_account():
        """تسجيل وتوصيل الحساب"""
        try:
            # إنشاء حساب جديد في MetaApi
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}_{int(time.time())}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': password,
                'server': server,
                'magic': 999111
            })
            
            # تجهيز الحساب
            await account.deploy()
            await account.wait_connected(timeout_in_seconds=30)
            
            # حفظ الجلسة
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
                'daily_profit': 0,
                'daily_loss': 0,
                'balance': 0,
                'equity': 0,
                'drawdown': 0,
                'positions_count': 0,
                'max_trades_per_day': 4,
                'trades_today': 0,
                'initial_balance': 0  # سيتم تحديثه لاحقاً
            }
            
            update_session(session_id, session_data)
            
            return {
                "status": "success",
                "session_id": session_id,
                "account_id": account.id,
                "message": "✅ تم الاتصال بنجاح!"
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"❌ خطأ في الاتصال: {str(e)}"
            }
    
    result = run_async(register_account())
    
    if result['status'] == 'success':
        return jsonify(result), 201
    else:
        return jsonify(result), 500


@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    """🔥 الـ ENDPOINT الأساسي المتوافق مع Flutter: استخراج البيانات الحية"""
    session_id = request.args.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    if session.get('status') != 'connected':
        return jsonify({"status": "error", "message": "الحساب غير متصل"}), 400
    
    # فحص الحظر
    if session.get('is_locked'):
        unlock_time_str = session.get('unlock_at')
        try:
            unlock_time = datetime.fromisoformat(unlock_time_str)
            if datetime.now() < unlock_time:
                return jsonify({
                    "status": "locked",
                    "reason": session.get('lockdown_reason'),
                    "locked_until": session.get('unlock_at'),
                    "message": "🔒 الحساب مقفول حالياً"
                }), 200
            else:
                # فك الحظر تلقائياً
                session['is_locked'] = False
                update_session(session_id, session)
        except:
            pass
    
    async def fetch_data():
        """جلب البيانات من MetaTrader"""
        try:
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            
            # جلب الحالة الأساسية
            state = await account.get_account_information()
            positions = await account.get_positions()
            
            # حساب الأرقام
            balance = float(state.balance) if hasattr(state, 'balance') else 0.0
            equity = float(state.equity) if hasattr(state, 'equity') else balance
            current_pnl = equity - balance
            drawdown_percent = ((balance - equity) / balance * 100) if balance > 0 else 0.0
            
            # تحديث الرصيد الافتتاحي إذا لم يكن محدداً
            if session.get('initial_balance', 0) == 0 and balance > 0:
                session['initial_balance'] = balance
            
            initial_balance = session.get('initial_balance', balance) or balance
            overall_growth = ((balance - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0.0
            
            # تحديث الجلسة بالبيانات الحية
            session.update({
                'balance': balance,
                'equity': equity,
                'daily_profit': float(current_pnl) if current_pnl > 0 else 0,
                'daily_loss': float(current_pnl) if current_pnl < 0 else 0,
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
                    "overall_growth": float(overall_growth),
                    "open_trades": len(positions),
                    "is_locked": session.get('is_locked', False),
                    "last_update": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            print(f"❌ خطأ في جلب البيانات: {e}")
            return {
                "status": "error",
                "message": f"خطأ في الاتصال بالحساب: {str(e)}"
            }
    
    result = run_async(fetch_data())
    return jsonify(result), 200 if result['status'] == 'success' else 500


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    """قطع الاتصال بالحساب - يتوافق مع Flutter"""
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    async def undeploy_account():
        try:
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            await account.undeploy()
            
            session['status'] = 'disconnected'
            update_session(session_id, session)
            
            return {"status": "success", "message": "✅ تم قطع الاتصال"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    result = run_async(undeploy_account())
    return jsonify(result), 200 if result['status'] == 'success' else 500


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    """إغلاق طارئ - يتوافق مع Flutter"""
    data = request.json or {}
    session_id = data.get('session_id')
    reason = data.get('reason', 'manual')
    lockout_hours = data.get('lockout_hours', 2)
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    try:
        account_id = session.get('account_id')
        
        def _close():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            account = loop.run_until_complete(api.metatrader_account_api.get_account(account_id))
            if account.state != 'DEPLOYED':
                loop.run_until_complete(account.deploy())
            positions = loop.run_until_complete(account.get_positions())
            for position in positions:
                loop.run_until_complete(account.close_position_by_symbol(position.symbol))
            return True
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pool.submit(_close).result(timeout=30)
        
        session['is_locked'] = True
        session['locked_at'] = datetime.now().isoformat()
        session['unlock_at'] = (datetime.now() + timedelta(hours=lockout_hours)).isoformat()
        session['lockdown_reason'] = reason
        update_session(session_id, session)
        
        return jsonify({
            "status": "success",
            "message": f"🔒 تم الإغلاق الطارئ: {reason}"
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    """تحديث الأهداف - يتوافق مع Flutter"""
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
    
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    # تحديث القيم الموجودة
    if 'daily_profit_target' in data:
        session['daily_target'] = data['daily_profit_target']
    if 'daily_stop_loss' in data:
        session['max_loss_limit'] = -abs(data['daily_stop_loss'])
    if 'lockout_hours' in data:
        session['lockout_hours'] = data['lockout_hours']
    if 'early_warning' in data:
        session['early_warning'] = data['early_warning']
    
    update_session(session_id, session)
    
    return jsonify({
        "status": "success",
        "message": "✅ تم تحديث الإعدادات"
    }), 200


@app.route('/api/unlock/<session_id>', methods=['POST'])
def manual_unlock(session_id):
    """فك الحظر يدوياً (للاختبار)"""
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    session['is_locked'] = False
    update_session(session_id, session)
    
    return jsonify({"status": "success", "message": "✅ تم فك الحظر"}), 200


@app.route('/health', methods=['GET'])
def health_check():
    """فحص صحة السيرفر"""
    return jsonify({
        "status": "✅ السيرفر يعمل",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(SHARED_SESSION)
    }), 200


# ==================== تشغيل التطبيق ====================
if __name__ == '__main__':
    # تحميل الجلسات السابقة
    load_sessions()
    
    # بدء خيط Risk Monitor
    risk_monitor = RiskMonitor()
    risk_monitor.start()
    
    # تشغيل السيرفر
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
