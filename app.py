import os
import asyncio
import time
import json
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# جلب الـ Tokens من متغيرات البيئة في Render
# ---------------------------------------------------------------------------
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')
REDIS_URL = os.environ.get('REDIS_URL')

# ---------------------------------------------------------------------------
# التعامل مع Redis بشكل آمن (في حال عدم وجوده)
# ---------------------------------------------------------------------------
try:
    import redis
    if REDIS_URL:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        print("✅ Redis connected successfully")
    else:
        redis_client = None
        print("⚠️ REDIS_URL not set, using in-memory storage")
except Exception as e:
    redis_client = None
    print(f"⚠️ Redis connection failed: {e}, using in-memory storage")

# ---------------------------------------------------------------------------
# مخزن مؤقت للجلسات في حال عدم وجود Redis
# ---------------------------------------------------------------------------
memory_sessions = {}

def save_session(session_id, data):
    """حفظ الجلسة في Redis أو الذاكرة المؤقتة"""
    if redis_client:
        redis_client.setex(f"session:{session_id}", 7200, json.dumps(data))
    else:
        memory_sessions[session_id] = data

def get_session(session_id):
    """استرجاع الجلسة من Redis أو الذاكرة المؤقتة"""
    if redis_client:
        data = redis_client.get(f"session:{session_id}")
        return json.loads(data) if data else None
    return memory_sessions.get(session_id)

def delete_session(session_id):
    """حذف الجلسة من Redis أو الذاكرة المؤقتة"""
    if redis_client:
        redis_client.delete(f"session:{session_id}")
    else:
        memory_sessions.pop(session_id, None)

# ---------------------------------------------------------------------------
# دالة مساعدة للوصول الآمن إلى ID الصفقة (Dict vs Object)
# ---------------------------------------------------------------------------
def safe_get_position_id(pos):
    """استخراج ID الصفقة بغض النظر عن نوعها (Dict أو Object)"""
    if isinstance(pos, dict):
        return pos.get('id')
    return getattr(pos, 'id', None)

# ---------------------------------------------------------------------------
# دالة مساعدة لتشغيل الدالات غير المتزامنة (Async) داخل بيئة Flask المتزامنة
# ---------------------------------------------------------------------------
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# ---------------------------------------------------------------------------
# 1. دالة تسجيل الدخول والربط (Connect)
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
        return jsonify({"status": "error", "message": "METAAPI_TOKEN غير معرف في سيرفر Render"}), 500
    
    try:
        async def register_account():
    api = MetaApi(API_TOKEN)
    account = None
    
    # محاولة البحث عن حساب موجود
    try:
        print("🔄 Checking existing accounts on MetaApi...")
        existing_accounts = await api.metatrader_account_api.get_accounts()
        for acc in existing_accounts:
            acc_login = None
            if isinstance(acc, dict):
                acc_login = acc.get('login')
            else:
                acc_login = getattr(acc, 'login', None)
            if acc_login and str(acc_login) == str(login):
                account = acc
                print(f"♻️ Found existing account for login: {login}")
                break
    except Exception as e:
        print(f"⚠️ Could not fetch existing accounts: {e}")
    
    # إنشاء حساب جديد إذا لم يكن موجوداً
    if not account:
        print(f"✨ Creating new MetaApi account for login: {login}")
        try:
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': str(password),
                'server': str(server),
                'magic': 999111,
                'keywords': ['trading-guardian']
            })
        except Exception as create_error:
            print(f"❌ Account creation failed: {create_error}")
            raise Exception(f"فشل إنشاء الحساب في MetaApi: {create_error}")
    
    # نشر الحساب
    try:
        account_state = account.state if hasattr(account, 'state') else account.get('state', '')
        account_id = account.id if hasattr(account, 'id') else account.get('id')
    except:
        account_state = 'UNKNOWN'
        account_id = getattr(account, 'id', None)
        
    if account_state != 'DEPLOYED':
        print(f"⏳ Deploying account {account_id}...")
        await account.deploy()
    
    # انتظار الاتصال بالبروكر (مع زيادة المهلة إلى 60 ثانية)
    print(f"⏳ Waiting for account to connect to broker (timeout: 60s)...")
    try:
        await account.wait_connected(timeout_in_seconds=60)
    except Exception as conn_error:
        print(f"❌ Account failed to connect to broker: {conn_error}")
        # إذا فشل الاتصال، فالمستخدم أدخل بيانات خاطئة
        raise Exception("فشل الاتصال بالبروكر. تأكد من رقم الحساب، كلمة المرور، واسم السيرفر بدقة.")
    
    # إنشاء اتصال RPC
    connection = account.get_rpc_connection()
    await connection.connect()
    await connection.wait_synchronized()
    
    # جلب معلومات الحساب
    account_info = await connection.get_account_information()
    initial_balance = float(account_info.get('balance', 0.0))
    
    # إنشاء الجلسة وحفظها
    session_id = f"session_{login}_{int(time.time())}"
    session_data = {
        'session_id': session_id,
        'account_id': account_id,
        'login': str(login),
        'server': str(server),
        'status': 'connected',
        'connected_at': datetime.now().isoformat(),
        'daily_target': 500.0,
        'max_loss_limit': -500.0,
        'is_locked': False,
        'daily_profit': 0.0,
        'daily_loss': 0.0,
        'balance': initial_balance,
        'equity': initial_balance,
        'initial_balance': initial_balance
    }
    
    save_session(session_id, session_data)
    
    return {
        "status": "success",
        "session_id": session_id,
        "account_id": account_id,
        "message": "تم الاتصال وتأمين الحساب بنجاح!"
    }
        
        result = run_async(register_account())
        if result and result.get('status') == 'success':
            return jsonify(result), 201
        else:
            return jsonify({"status": "error", "message": result.get('message', 'فشلت عملية التهيئة')}), 500
            
    except Exception as e:
        print(f"❌ Connection API Critical Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 2. دالة الإحصائيات والمراقبة الصارمة (تغلق أي صفقة جديدة)
# ---------------------------------------------------------------------------
@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "خطأ في المصادقة: الجلسة منتهية"}), 401

    try:
        async def fetch_stats_and_enforce_lock():
            api = MetaApi(API_TOKEN)
            account = await api.metatrader_account_api.get_account(session['account_id'])
            
            if account.state != 'DEPLOYED':
                await account.deploy()
                
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            account_info = await connection.get_account_information()
            positions = await connection.get_positions()
            
            balance = float(account_info.get('balance', 0.0))
            equity = float(account_info.get('equity', 0.0))
            pnl = equity - balance
            
            initial = float(session.get('initial_balance', balance))
            drawdown_percent = abs((pnl / balance) * 100) if pnl < 0 and balance > 0 else 0.0
            overall_growth = ((balance - initial) / initial) * 100 if initial > 0 else 0.0
            
            session['balance'] = balance
            session['equity'] = equity

            # 🛑 المراقبة الصارمة: إذا قفلنا الحساب ووجدت صفقات مفتوحة
            if session.get('is_locked'):
                if len(positions) > 0:
                    print(f"🚨 LOCKOUT VIOLATION! User opened {len(positions)} trades. Closing them now!")
                    for pos in positions:
                        pos_id = safe_get_position_id(pos)
                        if pos_id:
                            try:
                                await connection.close_position(pos_id)
                            except:
                                try:
                                    await connection.cancel_order(pos_id)
                                except:
                                    pass
                    positions = []

            # 📈 الفحص التلقائي للهدف والخسارة
            if not session.get('is_locked'):
                daily_target = session.get('daily_target', 500.0)
                max_loss_limit = session.get('max_loss_limit', -500.0)
                
                if pnl >= daily_target or pnl <= max_loss_limit:
                    print(f"🎯 Target or Stop reached on server side! Locking down account.")
                    session['is_locked'] = True
                    for pos in positions:
                        pos_id = safe_get_position_id(pos)
                        if pos_id:
                            try:
                                await connection.close_position(pos_id)
                            except:
                                pass
                    positions = []
                    
                    # تحديث الجلسة بعد القفل
                    save_session(session_id, session)

            # تحديث الجلسة مع أحدث البيانات
            save_session(session_id, session)

            return {
                "status": "success",
                "data": {
                    "is_locked": session.get('is_locked', False),
                    "balance": balance,
                    "equity": equity,
                    "current_pnl": 0.0 if session.get('is_locked') else pnl,
                    "drawdown_percent": 0.0 if session.get('is_locked') else drawdown_percent,
                    "daily_profit": session.get('daily_profit', 0.0),
                    "overall_growth": overall_growth,
                    "open_trades": len(positions)
                }
            }

        result = run_async(fetch_stats_and_enforce_lock())
        return jsonify(result), 200

    except Exception as e:
        print(f"❌ Stats API Error: {e}")
        return jsonify({
            "status": "success",
            "data": {
                "is_locked": session.get('is_locked', False),
                "balance": session.get('balance', 0.0),
                "equity": session.get('equity', 0.0),
                "current_pnl": 0.0,
                "drawdown_percent": 0.0,
                "daily_profit": session.get('daily_profit', 0.0),
                "overall_growth": 0.0,
                "open_trades": 0
            }
        }), 200

# ---------------------------------------------------------------------------
# 3. دالة تحديث قيم الأهداف (Update Targets)
# ---------------------------------------------------------------------------
@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    if 'daily_profit_target' in data:
        session['daily_target'] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        session['max_loss_limit'] = -abs(float(data['daily_stop_loss']))
    if 'early_warning' in data:
        session['early_warning'] = bool(data['early_warning'])
        
    save_session(session_id, session)
        
    return jsonify({"status": "success", "message": "تم تحديث الأهداف بنجاح"}), 200

# ---------------------------------------------------------------------------
# 4. دالة الإغلاق الطارئ (Emergency Close)
# ---------------------------------------------------------------------------
@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    data = request.json or {}
    session_id = data.get('session_id')
    reason = data.get('reason', 'تم تفعيل الحماية الطارئة')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    session['is_locked'] = True
    save_session(session_id, session)
    
    try:
        async def close_all_positions():
            api = MetaApi(API_TOKEN)
            account = await api.metatrader_account_api.get_account(session['account_id'])
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            positions = await connection.get_positions()
            for pos in positions:
                pos_id = safe_get_position_id(pos)
                if pos_id:
                    try:
                        await connection.close_position(pos_id)
                    except:
                        pass
            return {"status": "success", "message": f"تم تفعيل القفل بنجاح: {reason}"}
            
        result = run_async(close_all_positions())
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 5. دالة قطع الاتصال ومسح الجلسة (Disconnect)
# ---------------------------------------------------------------------------
@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    data = request.json or {}
    session_id = data.get('session_id')
    if session_id:
        delete_session(session_id)
    return jsonify({"status": "success", "message": "تم فصل الجلسة بنجاح"}), 200

# ---------------------------------------------------------------------------
# 6. نقطة الصحة (Health Check) لـ Render
# ---------------------------------------------------------------------------
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200

# ---------------------------------------------------------------------------
# 7. نقطة رئيسية (Root) للتحقق من عمل السيرفر
# ---------------------------------------------------------------------------
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "online",
        "service": "Trading Guardian API",
        "timestamp": datetime.now().isoformat(),
        "endpoints": [
            "/health",
            "/api/connect",
            "/api/account-stats",
            "/api/update-targets",
            "/api/emergency-close",
            "/api/disconnect"
        ]
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
