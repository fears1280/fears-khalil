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
# 🔐 الحارس الخلفي: يتصل بـ MetaApi، يحسب المؤشرات، ويحفظ كاش للفلوتر
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
                        # 1. التحقق من انتهاء مدة القفل لإعادة التفعيل
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
                                    session["latest_stats"] = None # تصفير الكاش القديم لتحديثه
                                    save_session_data(session)
                            await asyncio.sleep(4)
                            continue

                        # 2. جلب البيانات الحية وتحديث الكاش وفحص الأهداف
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
                            
                            # حساب التراجع المئوي الحي
                            drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
                            remaining_trades = max(0, 4 - len(positions))
                            
                            # تحديث الكاش فوراً ليقرأه الفلوتر بسرعة البرق بدون انتظار
                            session["latest_stats"] = {
                                "is_locked": False,
                                "balance": balance,
                                "equity": equity,
                                "total_progress_drawdown": max(0.0, float(drawdown)),
                                "daily_profit": current_pnl,
                                "remaining_trades": remaining_trades
                            }
                            save_session_data(session)
                            
                            # فحص الأهداف القياسية
                            profit_target = float(session.get("profit_target", 0.0))
                            max_loss = float(session.get("max_loss", 0.0))
                            lock_minutes = int(session.get("lock_duration_minutes", 60))
                            
                            trigger_lock = False
                            
                            if profit_target > 0 and current_pnl >= profit_target:
                                trigger_lock = True
                                print(f"🎯 هدف الأرباح تحقق: {current_pnl}$")
                                
                            if max_loss > 0 and current_pnl <= -abs(max_loss):
                                trigger_lock = True
                                print(f"🛑 حد الخسارة تحقق: {current_pnl}$")
                            
                            if trigger_lock:
                                # تنفيذ خطة الطوارئ فورا وتصفية الحساب
                                await connection.close_all_positions()
                                lock_until_time = datetime.utcnow() + timedelta(minutes=lock_minutes)
                                
                                session["is_locked"] = True
                                session["lock_until"] = lock_until_time.isoformat()
                                # تصفير الأرقام في الكاش لأن الحساب سيقفل
                                session["latest_stats"] = {
                                    "is_locked": True,
                                    "balance": balance,
                                    "equity": balance,
                                    "total_progress_drawdown": 0.0,
                                    "daily_profit": 0.0,
                                    "remaining_trades": 0
                                }
                                save_session_data(session)
                                
                                await account.undeploy() # إغلاق برنامج الميتا وفصله تماماً
                                print(f"🔒 تم قفل الحساب وفصل الميتا بنجاح حتى: {lock_until_time}")
                                
                except Exception as e:
                    print(f"❌ خطأ في الحارس الخلفي: {e}")
                await asyncio.sleep(3) # تحديث حي كل 3 ثوانٍ

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

start_risk_monitor()

# ---------------------------------------------------------------------------
# 🚀 الروابط البرمجية (Endpoints) المستجيبة فورياً للفلوتر
# ---------------------------------------------------------------------------

@app.route('/api/connect', methods=['POST'])
def connect_user():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    platform = data.get('platform', 'mt5')
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "جميع البيانات مطلوبة"}), 400

    async def register():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
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
        await account.wait_connected()
        
        session = load_session_data()
        session["account_id"] = account.id
        session["is_locked"] = False
        session["latest_stats"] = None
        save_session_data(session)
        return {"status": "success", "accountId": account.id}

    try:
        result = run_async(register())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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


@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    """ ⚡ الرابط أصبح سريعاً جداً (يقرأ الكاش بلحظة دون الاتصال بـ MetaApi من الصفر) """
    session = load_session_data()
    
    if session.get("is_locked", False):
        return jsonify({
            "is_locked": True,
            "balance": 0.0,
            "equity": 0.0,
            "total_progress_drawdown": 0.0,
            "daily_profit": 0.0,
            "remaining_trades": 0
        }), 200

    # إرجاع الكاش المحدث الذي وفره الحارس الخلفي
    latest_stats = session.get("latest_stats")
    if latest_stats:
        return jsonify(latest_stats), 200

    # في حال لم يكتمل التحديث الأول بعد
    return jsonify({
        "is_locked": False,
        "balance": 0.0,
        "equity": 0.0,
        "total_progress_drawdown": 0.0,
        "daily_profit": 0.0,
        "remaining_trades": 4
    }), 200


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    session = load_session_data()
    account_id = session.get("account_id")
    if not account_id:
        return jsonify({"status": "error", "message": "لم يتم العثور على حساب"}), 400

    async def close_all():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.get_account(account_id)
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        await connection.close_all_positions()
        return {"status": "success", "message": "تم إغلاق الصفقات طارئاً"}

    try:
        result = run_async(close_all())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return jsonify({"status": "success", "message": "Disconnected"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
