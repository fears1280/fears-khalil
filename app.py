from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta

app = Flask(__name__)

SESSION_FILE = "guardian_session.json"

def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def save_session_data(data):
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)

def load_session_data():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

# ---------------------------------------------------------------------------
# 🔐 الحارس الخلفي: يتصل بـ MetaApi، يحدّث الكاش الفعلي، ويفحص الأهداف
# ---------------------------------------------------------------------------
def start_risk_monitor():
    def monitor_worker():
        async def check_risk_loop():
            while True:
                try:
                    session = load_session_data()
                    account_id = session.get("account_id")
                    is_locked = session.get("is_locked", False)
                    
                    if account_id:
                        # 1. التحقق من انتهاء مدة القفل
                        if is_locked:
                            lock_until_str = session.get("lock_until")
                            if lock_until_str:
                                lock_until = datetime.fromisoformat(lock_until_str)
                                if datetime.utcnow() >= lock_until:
                                    print("🔓 انتهت مدة القفل! إعادة تفعيل حساب الميتا...")
                                    token = os.getenv("METAAPI_TOKEN")
                                    api = MetaApi(token)
                                    account = await api.metatrader_account_api.get_account(account_id)
                                    await account.deploy()
                                    session["is_locked"] = False
                                    session["lock_until"] = None
                                    session["latest_stats"] = None
                                    save_session_data(session)
                            await asyncio.sleep(4)
                            continue

                        # 2. جلب البيانات الحقيقية من MetaApi وتحديث الكاش
                        token = os.getenv("METAAPI_TOKEN")
                        api = MetaApi(token)
                        account = await api.metatrader_account_api.get_account(account_id)
                        
                        if account.state == 'DEPLOYED':
                            connection = account.get_rpc_connection()
                            await connection.connect()
                            await connection.wait_synchronized()
                            
                            account_info = await connection.get_account_information()
                            positions = await connection.get_positions()
                            
                            balance = float(account_info.get('balance', 0.0))
                            equity = float(account_info.get('equity', 0.0))
                            current_pnl = equity - balance
                            
                            # حساب التراجع المئوي الحي بدقة
                            drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
                            open_trades = len(positions)
                            remaining_trades = max(0, 4 - open_trades)
                            
                            # تحديث الكاش بالبيانات الصحيحة التي ينتظرها الفلوتر
                            session["latest_stats"] = {
                                "is_locked": False,
                                "balance": balance,
                                "equity": equity,
                                "drawdown_percent": max(0.0, float(drawdown)),
                                "current_pnl": float(current_pnl),
                                "daily_profit": float(current_pnl), # تحديث قيمة الربح اليومي العائم
                                "open_trades": open_trades,
                                "remaining_trades": remaining_trades,
                                "overall_growth": ((equity - balance) / balance * 100) if balance > 0 else 0.0
                            }
                            save_session_data(session)
                            
                            # فحص الأهداف المحددة من المستخدم
                            profit_target = float(session.get("profit_target", 0.0))
                            max_loss = float(session.get("max_loss", 0.0))
                            lock_minutes = int(session.get("lock_duration_minutes", 60))
                            
                            trigger_lock = False
                            if profit_target > 0 and current_pnl >= profit_target:
                                trigger_lock = True
                            if max_loss > 0 and current_pnl <= -abs(max_loss):
                                trigger_lock = True
                            
                            if trigger_lock:
                                await connection.close_all_positions()
                                lock_until_time = datetime.utcnow() + timedelta(minutes=lock_minutes)
                                
                                session["is_locked"] = True
                                session["lock_until"] = lock_until_time.isoformat()
                                session["latest_stats"] = {
                                    "is_locked": True,
                                    "balance": balance,
                                    "equity": balance,
                                    "drawdown_percent": 0.0,
                                    "current_pnl": 0.0,
                                    "daily_profit": 0.0,
                                    "open_trades": 0,
                                    "remaining_trades": 0,
                                    "overall_growth": 0.0
                                }
                                save_session_data(session)
                                await account.undeploy() # فصل الحساب تماماً وإغلاق الميتا
                                
                except Exception as e:
                    print(f"❌ خطأ الحارس: {e}")
                await asyncio.sleep(3)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

start_risk_monitor()

# ---------------------------------------------------------------------------
# 🚀 الروابط (Endpoints) الصارمة والآمنة
# ---------------------------------------------------------------------------

@app.route('/api/connect', methods=['POST'])
def connect_user():
    """ 🔒 فحص حقيقي صارم للبيانات قبل السماح بالدخول """
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    platform = data.get('platform', 'mt5')
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "جميع البيانات مطلوبة"}), 400

    async def register_and_validate():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        
        # 1. إنشاء الحساب على سيرفر MetaApi
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login}',
            'type': 'cloud',
            'platform': platform,
            'login': str(login),
            'password': password,
            'server': server,
            'magic': 999111,
            'keywords': ['trading-guardian']
        })
        
        await account.deploy()
        
        # 2. ⚠️ اختبار الاتصال الفعلي بالبروكر للتحقق من كلمة السر والرقم
        try:
            await account.wait_connected(timeout_in_seconds=30)
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            # جلب أول قراءة للتأكد من نجاح المصادقة بالكامل
            account_info = await connection.get_account_information()
            
            # مسح البيانات القديمة تماماً وبدء جلسة نظيفة بحساب حقيقي
            session = {
                "account_id": account.id,
                "is_locked": False,
                "profit_target": 0.0,
                "max_loss": 0.0,
                "lock_duration_minutes": 60,
                "latest_stats": {
                    "is_locked": False,
                    "balance": float(account_info.get('balance', 0.0)),
                    "equity": float(account_info.get('equity', 0.0)),
                    "drawdown_percent": 0.0,
                    "current_pnl": 0.0,
                    "daily_profit": 0.0,
                    "open_trades": 0,
                    "remaining_trades": 4,
                    "overall_growth": 0.0
                }
            }
            save_session_data(session)
            return {"status": "success", "accountId": account.id}
            
        except Exception as conn_error:
            # 🛑 إذا كانت البيانات وهمية أو خاطئة، نقوم بحذف الحساب فوراً وإرجاع خطأ
            await account.cleanup()
            return {"status": "error", "message": "فشل الاتصال بالبروكر. تأكد من رقم الحساب، كلمة المرور، أو السيرفر الحقيقي."}

    try:
        result = run_async(register_and_validate())
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 401 # كود 401 يمنع الفلوتر من الدخول نهائياً
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    session = load_session_data()
    
    if session.get("is_locked", False):
        return jsonify({
            "is_locked": True,
            "balance": 0.0,
            "equity": 0.0,
            "drawdown_percent": 0.0,
            "current_pnl": 0.0,
            "daily_profit": 0.0,
            "open_trades": 0,
            "remaining_trades": 0,
            "overall_growth": 0.0
        }), 200

    latest_stats = session.get("latest_stats")
    if latest_stats:
        return jsonify(latest_stats), 200

    return jsonify({
        "is_locked": False,
        "balance": 0.0,
        "equity": 0.0,
        "drawdown_percent": 0.0,
        "current_pnl": 0.0,
        "daily_profit": 0.0,
        "open_trades": 0,
        "remaining_trades": 4,
        "overall_growth": 0.0
    }), 200


@app.route('/api/set-targets', methods=['POST'])
def set_targets():
    data = request.json or {}
    session = load_session_data()
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب نشط"}), 400
        
    session["profit_target"] = float(data.get('profit_target', 0.0))
    session["max_loss"] = float(data.get('max_loss', 0.0))
    session["lock_duration_minutes"] = int(data.get('lock_duration_minutes', 60))
    save_session_data(session)
    return jsonify({"status": "success", "message": "تم تحديث الأهداف بنجاح"})


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return jsonify({"status": "success", "message": "Disconnected"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
