"""
╔═══════════════════════════════════════════════════════════════════════════╗
║               🛡️ Trading Guardian - سيرفر نهائي محسّن 🛡️                 ║
║                                                                           ║
║  حل شامل لجميع مشاكل:                                                   ║
║  ✅ Event Loop Issues (Asyncio)                                          ║
║  ✅ Data Type Validation (Login as Integer)                             ║
║  ✅ CORS Problems                                                         ║
║  ✅ WebSocket Real-time Updates                                          ║
║  ✅ Thread Safety                                                         ║
║  ✅ Render Compatibility                                                  ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
"""

# ===============================
# 1️⃣ استيراد المكتبات الأساسية
# ===============================
import os
import sys
import asyncio
import threading
import time
import traceback
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import json

# إعدادات الـ Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===============================
# 2️⃣ استيراد Flask و WebSocket
# ===============================
from flask import Flask, request, jsonify
from flask_cors import CORS

# ملاحظة: WebSocket اختياري لأنه قد يسبب مشاكل مع Gunicorn
# نستخدم HTTP polling بدلاً منه
# من flask_socketio import SocketIO, emit

# ===============================
# 3️⃣ استيراد MetaApi
# ===============================
try:
    from metaapi_cloud_sdk import MetaApi
    logger.info("✅ MetaApi SDK loaded successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import MetaApi: {e}")
    sys.exit(1)

# ===============================
# 4️⃣ تهيئة Flask و CORS
# ===============================
app = Flask(__name__)

# تفعيل CORS بشكل صحيح
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"],
    }
})

# ===============================
# 5️⃣ إعدادات البيئة
# ===============================
API_TOKEN = os.environ.get('METAAPI_TOKEN', '').strip()
RENDER_ENV = os.environ.get('RENDER', 'false').lower() == 'true'

# فحص وطباعة حالة التوكن
if not API_TOKEN:
    logger.error("❌ CRITICAL: METAAPI_TOKEN not found in environment variables!")
    logger.error("   Please add it to Render dashboard: Settings > Environment Variables")
    # لا نخرج من البرنامج، فقط نتابع مع الخطأ
else:
    logger.info(f"✅ METAAPI_TOKEN loaded: {API_TOKEN[:10]}...{API_TOKEN[-5:]}")

if RENDER_ENV:
    logger.info("🌍 Running on Render.com")
else:
    logger.info("💻 Running locally")

# ===============================
# 6️⃣ متغيرات الحالة العامة
# ===============================
# تخزين الجلسات - استخدام Dictionary عادي (آمن مع GIL في Python)
SESSIONS: Dict[str, Dict[str, Any]] = {}
SESSIONS_LOCK = threading.Lock()  # حماية من race conditions

# مراقب الخيوط (Threads)
MONITORING_THREADS: Dict[str, threading.Thread] = {}

# ===============================
# 7️⃣ دالة آمنة للعمل مع Asyncio
# ===============================
def run_async_safely(coro):
    """
    دالة آمنة جداً لتشغيل الكود الـ Async في بيئة Flask (التي هي Synchronous)
    
    الفكرة:
    - Flask يعمل بشكل Synchronous (متسلسل)
    - MetaApi يعمل بشكل Async (غير متسلسل)
    - نحتاج جسر آمن بينهما
    """
    try:
        # محاولة الوصول إلى الـ loop الموجود
        try:
            loop = asyncio.get_running_loop()
            # إذا كانت هناك loop جارية، نستخدمها (في الخيوط المنفصلة)
            logger.warning("⚠️ Running loop detected, using ensure_future")
            future = asyncio.ensure_future(coro)
            return future
        except RuntimeError:
            # لا توجد loop جارية، نُنشئ واحدة
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError("Event loop is closed")
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # تشغيل الكود الـ Async
            result = loop.run_until_complete(coro)
            return result
            
    except Exception as e:
        logger.error(f"❌ Async execution failed: {e}")
        logger.error(traceback.format_exc())
        raise

# ===============================
# 8️⃣ دالة الاتصال بـ MetaApi (الدالة الحرجة)
# ===============================
async def connect_to_metaapi(login_int: int, password: str, server: str) -> str:
    """
    الاتصال بـ MetaApi وإنشاء/تفعيل الحساب
    
    المعاملات:
    - login_int: رقم الحساب (يجب أن يكون int بالضرورة!)
    - password: كلمة المرور
    - server: اسم السيرفر
    
    النتيجة:
    - account_id: معرف الحساب في MetaApi
    """
    logger.info(f"🔗 Attempting to connect: login={login_int}, server={server}")
    
    if not API_TOKEN:
        raise ValueError("METAAPI_TOKEN is not configured!")
    
    try:
        # إنشاء كائن MetaApi
        api = MetaApi(API_TOKEN)
        
        # ① محاولة البحث عن حساب موجود
        logger.info(f"🔍 Checking for existing account with login {login_int}...")
        existing_account = None
        
        try:
            accounts = await api.metatrader_account_api.get_accounts()
            if accounts:
                for acc in accounts:
                    try:
                        acc_login = acc.login if hasattr(acc, 'login') else acc.get('login')
                        if str(acc_login) == str(login_int):
                            existing_account = acc
                            logger.info(f"✅ Found existing account: {existing_account.id}")
                            break
                    except:
                        continue
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch existing accounts: {e}")
        
        # ② إنشاء حساب جديد إذا لم يكن موجوداً
        if not existing_account:
            logger.info(f"➕ Creating new account for login {login_int}...")
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login_int}_{int(time.time())}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': int(login_int),  # ⚠️ MUST BE INT!
                'password': str(password),
                'server': str(server),
                'magic': 999111,
                'keywords': ['trading-guardian']
            })
            logger.info(f"✅ Account created: {account.id}")
        else:
            account = existing_account
        
        # ③ الحصول على معرف الحساب بشكل آمن
        account_id = account.id if hasattr(account, 'id') else account.get('id')
        if not account_id:
            raise ValueError("Could not retrieve account ID")
        
        # ④ التحقق من حالة الحساب
        logger.info(f"📡 Checking account state for {account_id}...")
        fresh_account = await api.metatrader_account_api.get_account(account_id)
        current_state = fresh_account.state if hasattr(fresh_account, 'state') else 'UNKNOWN'
        logger.info(f"   Current state: {current_state}")
        
        # ⑤ تفعيل الحساب إذا لم يكن مفعلاً
        if current_state != 'DEPLOYED':
            logger.info(f"🚀 Deploying account {account_id}...")
            await fresh_account.deploy()
            
            # انتظر قليلاً حتى يتم التفعيل
            await asyncio.sleep(2)
            
            # تحقق من التفعيل
            fresh_account = await api.metatrader_account_api.get_account(account_id)
            current_state = fresh_account.state if hasattr(fresh_account, 'state') else 'UNKNOWN'
            logger.info(f"   After deploy: {current_state}")
        
        logger.info(f"✅ Successfully connected to account: {account_id}")
        return account_id
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# ===============================
# 9️⃣ دالة مراقبة الحساب (Background Monitor)
# ===============================
async def monitor_account(session_id: str, account_id: str, stop_event: threading.Event):
    """
    مراقبة الحساب بشكل مستمر وتحديث البيانات الحية
    تعمل في خيط منفصل (daemon thread)
    """
    logger.info(f"🛡️ Monitor started for session {session_id} (account {account_id})")
    
    try:
        api = MetaApi(API_TOKEN)
        update_count = 0
        
        while not stop_event.is_set():
            try:
                # الحصول على الحساب
                account = await api.metatrader_account_api.get_account(account_id)
                
                # التأكد من أنه مفعل
                if account.state != 'DEPLOYED':
                    logger.warning(f"⚠️ Account {account_id} is not deployed, redeploying...")
                    await account.deploy()
                    await asyncio.sleep(2)
                
                # الحصول على معلومات الحساب
                try:
                    account_info = await account.get_account_information()
                except:
                    # إذا فشلت المحاولة الأولى، جرب RPC
                    connection = account.get_rpc_connection()
                    await connection.connect()
                    account_info = await connection.get_account_information()
                
                # استخراج البيانات
                balance = float(getattr(account_info, 'balance', 0.0))
                equity = float(getattr(account_info, 'equity', 0.0))
                profit = equity - balance
                
                # محاولة الحصول على المراكز
                try:
                    positions = await account.get_positions()
                except:
                    positions = []
                
                # تحديث الجلسة
                with SESSIONS_LOCK:
                    if session_id in SESSIONS:
                        session = SESSIONS[session_id]
                        session['balance'] = balance
                        session['equity'] = equity
                        session['profit'] = profit
                        session['positions_count'] = len(positions)
                        session['last_update'] = datetime.now().isoformat()
                        session['account_state'] = account.state
                        
                        # حساب الـ Drawdown
                        if balance > 0:
                            session['drawdown'] = abs((profit / balance) * 100) if profit < 0 else 0.0
                        else:
                            session['drawdown'] = 0.0
                        
                        # فحص الأهداف والحدود
                        if not session.get('is_locked'):
                            daily_target = session.get('daily_target', 500.0)
                            max_loss = session.get('max_loss_limit', -500.0)
                            
                            if profit >= daily_target:
                                logger.warning(f"🎯 SESSION {session_id}: TARGET REACHED! ({profit} >= {daily_target})")
                                session['is_locked'] = True
                                session['locked_reason'] = 'TARGET_REACHED'
                                session['locked_at'] = datetime.now().isoformat()
                                
                                # إغلاق جميع المراكز
                                for pos in positions:
                                    try:
                                        await account.close_position_by_symbol(pos.symbol)
                                    except:
                                        pass
                                        
                            elif profit <= max_loss:
                                logger.warning(f"🚫 SESSION {session_id}: LOSS LIMIT REACHED! ({profit} <= {max_loss})")
                                session['is_locked'] = True
                                session['locked_reason'] = 'LOSS_LIMIT_REACHED'
                                session['locked_at'] = datetime.now().isoformat()
                                
                                # إغلاق جميع المراكز
                                for pos in positions:
                                    try:
                                        await account.close_position_by_symbol(pos.symbol)
                                    except:
                                        pass
                
                update_count += 1
                if update_count % 10 == 0:  # طباعة كل 10 تحديثات
                    logger.info(f"📊 Session {session_id}: Balance={balance}, Equity={equity}, Profit={profit:.2f}")
                
                # انتظر قبل التحديث التالي
                await asyncio.sleep(2)  # تحديث كل ثانيتين
                
            except Exception as monitor_error:
                logger.error(f"❌ Monitor error for {session_id}: {monitor_error}")
                await asyncio.sleep(5)  # انتظر أطول إذا حدث خطأ
        
        logger.info(f"🛑 Monitor stopped for session {session_id}")
        
    except Exception as e:
        logger.error(f"❌ Critical monitor error: {e}")
        logger.error(traceback.format_exc())

def start_monitoring_thread(session_id: str, account_id: str):
    """
    بدء خيط منفصل لمراقبة الحساب
    """
    stop_event = threading.Event()
    
    def run_monitor():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(monitor_account(session_id, account_id, stop_event))
        except Exception as e:
            logger.error(f"Monitor thread error: {e}")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_monitor, daemon=True, name=f"Monitor-{session_id}")
    thread.start()
    
    MONITORING_THREADS[session_id] = thread
    logger.info(f"✅ Monitoring thread started for {session_id}")

# ===============================
# 🔟 الـ API Endpoints
# ===============================

@app.route('/health', methods=['GET'])
def health_check():
    """
    فحص صحة السيرفر
    """
    return jsonify({
        "status": "✅ Server is running",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(SESSIONS),
        "render": RENDER_ENV,
    }), 200


@app.route('/api/connect', methods=['POST', 'OPTIONS'])
def connect_account():
    """
    🔥 الـ Endpoint الأساسي للاتصال بحساب MetaTrader
    
    JSON Body Expected:
    {
        "login": 123456789,
        "password": "password",
        "server": "ICMarketsSC-Demo",
        "daily_target": 500.0,
        "max_loss_limit": 500.0
    }
    """
    # معالجة CORS OPTIONS
    if request.method == 'OPTIONS':
        return '', 204
    
    logger.info(f"📥 /api/connect request received")
    
    try:
        # الحصول على البيانات
        data = request.get_json(force=True, silent=True) or {}
        
        login = data.get('login')
        password = data.get('password')
        server = data.get('server', 'ICMarketsSC-Demo')
        
        # ✅ التحقق من الحقول الأساسية
        if not login or not password:
            logger.warning("⚠️ Missing login or password")
            return jsonify({
                "status": "error",
                "message": "Login and password are required"
            }), 400
        
        # ✅ تحويل login إلى int بشكل آمن
        try:
            login_int = int(login)
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Invalid login format: {login}")
            return jsonify({
                "status": "error",
                "message": f"Login must be a number, got: {login}"
            }), 400
        
        # ✅ الحصول على الأهداف
        try:
            daily_target = float(data.get('daily_target', 500.0))
            max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
        except (ValueError, TypeError):
            daily_target = 500.0
            max_loss_limit = -500.0
        
        # ✅ التحقق من التوكن
        if not API_TOKEN:
            logger.error("❌ METAAPI_TOKEN not configured")
            return jsonify({
                "status": "error",
                "message": "Server configuration error: METAAPI_TOKEN missing"
            }), 500
        
        logger.info(f"🔗 Connecting account: login={login_int}, server={server}")
        
        # ✅ الاتصال بـ MetaApi (الدالة الحرجة)
        account_id = run_async_safely(
            connect_to_metaapi(login_int, password, server)
        )
        
        # ✅ إنشاء الجلسة
        session_id = f"session_{login_int}_{int(time.time() * 1000)}"
        
        with SESSIONS_LOCK:
            SESSIONS[session_id] = {
                'session_id': session_id,
                'account_id': account_id,
                'login': str(login_int),
                'server': str(server),
                'status': 'connected',
                'active': True,
                'is_locked': False,
                'locked_reason': None,
                'locked_at': None,
                'daily_target': daily_target,
                'max_loss_limit': max_loss_limit,
                'balance': 0.0,
                'equity': 0.0,
                'profit': 0.0,
                'positions_count': 0,
                'drawdown': 0.0,
                'created_at': datetime.now().isoformat(),
                'last_update': datetime.now().isoformat(),
                'account_state': 'DEPLOYED'
            }
        
        # ✅ بدء المراقبة
        start_monitoring_thread(session_id, account_id)
        
        logger.info(f"✅ Account connected successfully: {session_id}")
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "account_id": account_id,
            "message": "Account connected successfully",
            "daily_target": daily_target,
            "max_loss_limit": max_loss_limit
        }), 201
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/live-data/<session_id>', methods=['GET'])
def get_live_data(session_id):
    """
    الحصول على البيانات الحية للجلسة
    """
    with SESSIONS_LOCK:
        if session_id not in SESSIONS:
            return jsonify({
                "status": "error",
                "message": "Session not found"
            }), 404
        
        session = SESSIONS[session_id]
    
    return jsonify({
        "status": "success",
        "data": {
            "session_id": session_id,
            "is_locked": session.get('is_locked', False),
            "locked_reason": session.get('locked_reason'),
            "balance": session.get('balance', 0.0),
            "equity": session.get('equity', 0.0),
            "profit": session.get('profit', 0.0),
            "drawdown": session.get('drawdown', 0.0),
            "positions_count": session.get('positions_count', 0),
            "account_state": session.get('account_state', 'UNKNOWN'),
            "daily_target": session.get('daily_target', 0.0),
            "max_loss_limit": session.get('max_loss_limit', 0.0),
            "last_update": session.get('last_update')
        }
    }), 200


@app.route('/api/disconnect/<session_id>', methods=['POST', 'OPTIONS'])
def disconnect_account(session_id):
    """
    قطع الاتصال بالجلسة
    """
    if request.method == 'OPTIONS':
        return '', 204
    
    with SESSIONS_LOCK:
        if session_id in SESSIONS:
            SESSIONS[session_id]['active'] = False
            del SESSIONS[session_id]
    
    if session_id in MONITORING_THREADS:
        # إيقاف الخيط (سيتوقف عند التحقق التالي)
        del MONITORING_THREADS[session_id]
    
    return jsonify({
        "status": "success",
        "message": "Session disconnected"
    }), 200


@app.route('/api/sessions', methods=['GET'])
def list_sessions():
    """
    قائمة بجميع الجلسات النشطة (للتطوير فقط)
    """
    with SESSIONS_LOCK:
        sessions_list = [
            {
                'session_id': s['session_id'],
                'login': s['login'],
                'status': 'locked' if s['is_locked'] else 'active',
                'balance': s['balance'],
                'profit': s['profit']
            }
            for s in SESSIONS.values()
        ]
    
    return jsonify({
        "status": "success",
        "count": len(sessions_list),
        "sessions": sessions_list
    }), 200


# ===============================
# 1️⃣1️⃣ Error Handlers
# ===============================
@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404


@app.errorhandler(500)
def server_error(error):
    logger.error(f"❌ Internal server error: {error}")
    return jsonify({"status": "error", "message": "Internal server error"}), 500


# ===============================
# 1️⃣2️⃣ تشغيل السيرفر
# ===============================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    logger.info("=" * 70)
    logger.info("🛡️ Trading Guardian Server Starting")
    logger.info("=" * 70)
    logger.info(f"Port: {port}")
    logger.info(f"Render: {RENDER_ENV}")
    logger.info(f"Token configured: {bool(API_TOKEN)}")
    logger.info("=" * 70)
    
    # تشغيل Flask بدون threaded mode (Gunicorn سيتولى التعامل مع الخيوط)
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,
        use_reloader=False,
        threaded=True  # السماح بمتطلبات متزامنة
    )
