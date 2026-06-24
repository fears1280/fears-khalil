from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
import time
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
    """تشغيل عمليات async بشكل آمن"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # إذا كانت الـ loop تعمل بالفعل، أنشئ task
            return asyncio.ensure_future(coro)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # إذا لم توجد loop، أنشئ واحدة جديدة
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
            for session_id, session_data in SHARED_SESSION.items():
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
            # إغلاق جميع الصفقات
            account_id = session.get('account_id')
            asyncio.run(self.close_all_positions(account_id))
            
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

@app.route('/api/connect-user', methods=['POST'])
def connect_user():
    """ربط حساب MetaTrader 5 جديد"""
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
                'trades_today': 0
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


@app.route('/api/get-live-data/<session_id>', methods=['GET'])
def get_live_data(session_id):
    """🔥 الـ ENDPOINT الأساسي: استخراج البيانات الحية"""
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404
    
    if session.get('status') != 'connected':
        return jsonify({"status": "error", "message": "الحساب غير متصل"}), 400
    
    # فحص الحظر
    if session.get('is_locked'):
        unlock_time = datetime.fromisoformat(session.get('unlock_at'))
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
    
    async def fetch_data():
        """جلب البيانات من MetaTrader"""
        try:
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            
            # جلب الحالة الأساسية
            state = await account.get_account_information()
            positions = await account.get_positions()
            
            # حساب الأرباح والخسائر
            balance = state.balance if hasattr(state, 'balance') else 0
            equity = state.equity if hasattr(state, 'equity') else balance
            profit = equity - balance
            drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0
            
            # تحديث الجلسة بالبيانات الحية
            session.update({
                'balance': float(balance),
                'equity': float(equity),
                'daily_profit': float(profit) if profit > 0 else 0,
                'daily_loss': float(profit) if profit < 0 else 0,
                'drawdown': float(abs(drawdown)),
                'positions_count': len(positions),
                'last_update': datetime.now().isoformat()
            })
            update_session(session_id, session)
            
            return {
                "status": "success",
                "data": {
                    "balance": float(balance),
                    "equity": float(equity),
                    "profit": float(profit),
                    "drawdown": float(abs(drawdown)),
                    "positions_count": len(positions),
                    "daily_target": session.get('daily_target'),
                    "max_loss_limit": session.get('max_loss_limit'),
                    "is_locked": session.get('is_locked'),
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


@app.route('/api/disconnect/<session_id>', methods=['POST'])
def disconnect_user(session_id):
    """قطع الاتصال بالحساب"""
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
