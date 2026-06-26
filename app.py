import os
import json
import threading
import time
import asyncio
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from upstash_redis import Redis  # مكتبة Upstash Redis الرسمية

# ==================== إعداد السجلات ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("guardian")

# ==================== تطبيق Flask ====================
app = Flask(__name__)
CORS(app)

# ==================== متغيرات البيئة والتحقق ====================
API_TOKEN = os.environ.get("METAAPI_TOKEN")
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not API_TOKEN:
    raise ValueError("METAAPI_TOKEN غير موجود في متغيرات البيئة!")
if not REDIS_URL or not REDIS_TOKEN:
    raise ValueError("UPSTASH_REDIS_REST_URL و UPSTASH_REDIS_REST_TOKEN مطلوبان!")

# ==================== توصيل Redis ====================
try:
    redis = Redis(url=REDIS_URL, token=REDIS_TOKEN)
    # اختبار الاتصال
    redis.ping()
    logger.info("✅ تم الاتصال بـ Upstash Redis بنجاح")
except Exception as e:
    logger.critical(f"فشل الاتصال بـ Redis: {e}")
    raise

# ==================== تهيئة MetaApi ====================
from metaapi_cloud_sdk import MetaApi
api = MetaApi(API_TOKEN)

# ==================== إدارة الجلسات عبر Redis ====================
SESSION_PREFIX = "session:"
SESSION_IDS_KEY = "session_ids"

def get_session(session_id: str) -> dict | None:
    """استرداد جلسة من Redis"""
    try:
        data = redis.get(f"{SESSION_PREFIX}{session_id}")
        return json.loads(data) if data else None
    except Exception as e:
        logger.error(f"خطأ في جلب الجلسة {session_id}: {e}")
        return None

def save_session(session_id: str, data: dict):
    """حفظ جلسة في Redis مع إضافتها لمجموعة الجلسات"""
    try:
        redis.set(f"{SESSION_PREFIX}{session_id}", json.dumps(data))
        redis.sadd(SESSION_IDS_KEY, session_id)
        logger.debug(f"تم حفظ الجلسة {session_id}")
    except Exception as e:
        logger.error(f"خطأ في حفظ الجلسة {session_id}: {e}")

def delete_session(session_id: str):
    """حذف جلسة من Redis"""
    try:
        redis.delete(f"{SESSION_PREFIX}{session_id}")
        redis.srem(SESSION_IDS_KEY, session_id)
        logger.debug(f"تم حذف الجلسة {session_id}")
    except Exception as e:
        logger.error(f"خطأ في حذف الجلسة {session_id}: {e}")

def get_all_sessions() -> dict:
    """استرداد جميع الجلسات (للمراقبة)"""
    try:
        ids = redis.smembers(SESSION_IDS_KEY)
        sessions = {}
        for sid in ids:
            data = get_session(sid)
            if data:
                sessions[sid] = data
        return sessions
    except Exception as e:
        logger.error(f"خطأ في جلب جميع الجلسات: {e}")
        return {}

# ==================== دالة تشغيل async آمنة ====================
def run_async(coro):
    """تشغيل coroutine باستخدام asyncio.run() (آمن في الخيوط)"""
    try:
        return asyncio.run(coro)
    except Exception as e:
        logger.exception("خطأ أثناء تنفيذ العملية غير المتزامنة")
        raise

# ==================== خيط مراقبة المخاطر ====================
class RiskMonitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
        self.check_interval = 4

    def run(self):
        logger.info("بدء مراقبة المخاطر...")
        while self.running:
            try:
                self.check_all_accounts()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"خطأ في مراقبة المخاطر: {e}")
                time.sleep(self.check_interval)

    def check_all_accounts(self):
        sessions = get_all_sessions()
        for session_id, session_data in sessions.items():
            if session_data.get('status') != 'connected':
                continue

            daily_loss = float(session_data.get('daily_loss', 0))
            daily_profit = float(session_data.get('daily_profit', 0))
            max_loss_limit = float(session_data.get('max_loss_limit', -500))
            daily_target = float(session_data.get('daily_target', 500))

            if daily_loss <= max_loss_limit:
                logger.warning(f"تم الوصول للخسارة القصوى {session_id}")
                self.emergency_lockdown(session_id, "reached_max_loss")
            elif daily_profit >= daily_target:
                logger.info(f"تم تحقيق الهدف اليومي {session_id}")
                self.emergency_lockdown(session_id, "reached_daily_target")

    def emergency_lockdown(self, session_id, reason):
        session = get_session(session_id)
        if not session:
            return
        try:
            account_id = session.get('account_id')
            # إغلاق جميع الصفقات
            run_async(self.close_all_positions(account_id))

            # تحديث حالة الجلسة
            session['is_locked'] = True
            session['locked_at'] = datetime.now().isoformat()
            session['unlock_at'] = (datetime.now() + timedelta(hours=2)).isoformat()
            session['lockdown_reason'] = reason
            save_session(session_id, session)
            logger.info(f"تم قفل الحساب {session_id} بسبب: {reason}")
        except Exception as e:
            logger.error(f"خطأ في قفل الحساب {session_id}: {e}")

    async def close_all_positions(self, account_id):
        """إغلاق جميع الصفقات المفتوحة"""
        try:
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
            positions = await account.get_positions()
            for position in positions:
                await account.close_position_by_symbol(position.symbol)
            logger.info(f"تم إغلاق صفقات الحساب {account_id}")
        except Exception as e:
            logger.error(f"خطأ في إغلاق الصفقات لـ {account_id}: {e}")

# ==================== نقاط النهاية API ====================

@app.route('/api/connect', methods=['POST'])
def connect():
    """إنشاء حساب MT5 جديد وربطه"""
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    daily_target = data.get('daily_target', 500)
    max_loss_limit = data.get('max_loss_limit', -500)

    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "يجب توفير login, password, server"}), 400

    try:
        async def register_account():
            # إنشاء الحساب في MetaApi
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}_{int(time.time())}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': password,
                'server': server,
                'magic': 999111
            })
            logger.info(f"تم إنشاء الحساب: {account.id}")

            # نشر الحساب وانتظار الاتصال
            await account.deploy()
            await account.wait_connected(timeout_in_seconds=30)
            logger.info(f"الحساب {account.id} متصل")

            # بناء بيانات الجلسة
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
                'initial_balance': 0
            }
            save_session(session_id, session_data)

            return {
                "status": "success",
                "session_id": session_id,
                "account_id": account.id,
                "message": "تم الاتصال بنجاح!"
            }

        result = run_async(register_account())
        status_code = 201 if result.get('status') == 'success' else 500
        return jsonify(result), status_code

    except Exception as e:
        logger.exception("فشل في /api/connect")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    """إحصائيات الحساب الحالية"""
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400

    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404

    if session.get('status') != 'connected':
        return jsonify({"status": "error", "message": "الحساب غير متصل"}), 400

    # التحقق من القفل المؤقت
    if session.get('is_locked'):
        unlock_time_str = session.get('unlock_at')
        if unlock_time_str:
            try:
                unlock_time = datetime.fromisoformat(unlock_time_str)
                if datetime.now() < unlock_time:
                    return jsonify({
                        "status": "locked",
                        "reason": session.get('lockdown_reason'),
                        "locked_until": unlock_time_str,
                        "message": "الحساب مقفول حالياً"
                    }), 200
                else:
                    # انتهت فترة القفل – فك الحظر
                    session['is_locked'] = False
                    save_session(session_id, session)
            except Exception as e:
                logger.warning(f"خطأ في تحليل unlock_at: {e}")

    try:
        async def fetch_data():
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)

            state = await account.get_account_information()
            positions = await account.get_positions()

            balance = float(state.balance) if hasattr(state, 'balance') else 0.0
            equity = float(state.equity) if hasattr(state, 'equity') else balance
            current_pnl = equity - balance
            drawdown_percent = ((balance - equity) / balance * 100) if balance > 0 else 0.0

            # الرصيد الأولي
            if session.get('initial_balance', 0) == 0 and balance > 0:
                session['initial_balance'] = balance
            initial_balance = session.get('initial_balance', balance) or balance
            overall_growth = ((balance - initial_balance) / initial_balance * 100) if initial_balance > 0 else 0.0

            # تحديث الجلسة
            session.update({
                'balance': balance,
                'equity': equity,
                'daily_profit': float(current_pnl) if current_pnl > 0 else 0,
                'daily_loss': float(current_pnl) if current_pnl < 0 else 0,
                'drawdown': float(abs(drawdown_percent)),
                'positions_count': len(positions),
                'last_update': datetime.now().isoformat()
            })
            save_session(session_id, session)

            return {
                "status": "success",
                "data": {
                    "balance": balance,
                    "equity": equity,
                    "current_pnl": float(current_pnl),
                    "drawdown_percent": float(abs(drawdown_percent)),
                    "daily_profit": session['daily_profit'],
                    "overall_growth": float(overall_growth),
                    "open_trades": len(positions),
                    "is_locked": session.get('is_locked', False),
                    "last_update": datetime.now().isoformat()
                }
            }

        result = run_async(fetch_data())
        return jsonify(result), 200 if result.get('status') == 'success' else 500

    except Exception as e:
        logger.exception("فشل في /api/account-stats")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    """إيقاف الحساب وقطع الاتصال"""
    data = request.json or {}
    session_id = data.get('session_id')
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400

    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404

    try:
        async def undeploy_account():
            account_id = session.get('account_id')
            account = await api.metatrader_account_api.get_account(account_id)
            await account.undeploy()
            session['status'] = 'disconnected'
            save_session(session_id, session)
            return {"status": "success", "message": "تم قطع الاتصال"}

        result = run_async(undeploy_account())
        return jsonify(result), 200 if result.get('status') == 'success' else 500

    except Exception as e:
        logger.exception("فشل في /api/disconnect")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    """إغلاق طارئ لجميع الصفقات وقفل الحساب"""
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

        async def close_positions():
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
            positions = await account.get_positions()
            for pos in positions:
                await account.close_position_by_symbol(pos.symbol)
            return True

        run_async(close_positions())

        session['is_locked'] = True
        session['locked_at'] = datetime.now().isoformat()
        session['unlock_at'] = (datetime.now() + timedelta(hours=lockout_hours)).isoformat()
        session['lockdown_reason'] = reason
        save_session(session_id, session)

        return jsonify({
            "status": "success",
            "message": f"تم الإغلاق الطارئ: {reason}"
        }), 200

    except Exception as e:
        logger.exception("فشل في /api/emergency-close")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    """تحديث أهداف الربح/الخسارة"""
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
    if 'early_warning' in data:
        session['early_warning'] = float(data['early_warning'])

    save_session(session_id, session)
    return jsonify({
        "status": "success",
        "message": "تم تحديث الإعدادات"
    }), 200


@app.route('/api/unlock/<session_id>', methods=['POST'])
def manual_unlock(session_id):
    """فك القفل يدوياً"""
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة غير موجودة"}), 404

    session['is_locked'] = False
    save_session(session_id, session)
    return jsonify({"status": "success", "message": "تم فك الحظر"}), 200


@app.route('/health', methods=['GET'])
def health_check():
    """فحص صحة السيرفر وعدد الجلسات النشطة"""
    try:
        active = len(get_all_sessions())
    except:
        active = -1
    return jsonify({
        "status": "السيرفر يعمل",
        "timestamp": datetime.now().isoformat(),
        "active_sessions": active
    }), 200


# ==================== بدء المراقبة ====================
risk_monitor = RiskMonitor()
risk_monitor.start()

# ==================== نقطة البداية (للتطوير المحلي فقط) ====================
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
