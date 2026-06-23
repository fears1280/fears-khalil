from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta, timezone

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
        json.dump(data, f, indent=2)

def load_session_data():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

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
                        # التحقق من انتهاء القفل
                        if is_locked:
                            lock_until_str = session.get("lock_until")
                            if lock_until_str:
                                lock_until = datetime.fromisoformat(lock_until_str)
                                if datetime.now(timezone.utc) >= lock_until:
                                    print("🔓 انتهت مدة القفل!")
                                    session["is_locked"] = False
                                    session["lock_until"] = None
                                    session["latest_stats"] = None
                                    save_session_data(session)
                            await asyncio.sleep(4)
                            continue

                        # جلب البيانات
                        token = os.getenv("METAAPI_TOKEN")
                        if not token:
                            print("❌ METAAPI_TOKEN غير موجود")
                            await asyncio.sleep(5)
                            continue
                            
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
                            drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
                            remaining_trades = max(0, 4 - len(positions))
                            
                            # تحديث الكاش
                            session["latest_stats"] = {
                                "is_locked": False,
                                "balance": balance,
                                "equity": equity,
                                "total_progress_drawdown": max(0.0, float(drawdown)),
                                "daily_profit": current_pnl,
                                "remaining_trades": remaining_trades
                            }
                            save_session_data(session)
                            
                            # فحص الأهداف
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
                                # إغلاق الصفقات
                                positions = await connection.get_positions()
                                for pos in positions:
                                    await connection.close_position(pos['id'])
                                    print(f"✓ تم إغلاق صفقة {pos['id']}")
                                
                                lock_until_time = datetime.now(timezone.utc) + timedelta(minutes=lock_minutes)
                                
                                session["is_locked"] = True
                                session["lock_until"] = lock_until_time.isoformat()
                                session["latest_stats"] = {
                                    "is_locked": True,
                                    "balance": balance,
                                    "equity": balance,
                                    "total_progress_drawdown": 0.0,
                                    "daily_profit": 0.0,
                                    "remaining_trades": 0
                                }
                                save_session_data(session)
                                print(f"🔒 تم قفل الحساب حتى: {lock_until_time}")
                                
                except Exception as e:
                    print(f"❌ خطأ: {e}")
                await asyncio.sleep(3)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

start_risk_monitor()

# ---------------------------------------------------------------------------
# ✅ نقاط API متوافقة مع تطبيق فلاتر
# ---------------------------------------------------------------------------

@app.route('/api/connect', methods=['POST'])
def connect_user():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "جميع البيانات مطلوبة"}), 400

    async def register():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': str(login),
            'password': password,
            'server': server,
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


@app.route('/api/update-targets', methods=['POST'])  # ✅ متوافق مع التطبيق
def update_targets():
    data = request.json or {}
    session = load_session_data()
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب نشط"}), 400
    
    # ✅ دعم أسماء الحقول من التطبيق
    if 'daily_profit_target' in data:
        session["profit_target"] = float(data['daily_profit_target'])
    elif 'profit_target' in data:
        session["profit_target"] = float(data['profit_target'])
    
    if 'daily_stop_loss' in data:
        session["max_loss"] = float(data['daily_stop_loss'])
    elif 'max_loss' in data:
        session["max_loss"] = float(data['max_loss'])
    
    if 'lockout_hours' in data:
        session["lock_duration_minutes"] = int(data['lockout_hours']) * 60
    elif 'lock_duration_minutes' in data:
        session["lock_duration_minutes"] = int(data['lock_duration_minutes'])
    
    save_session_data(session)
    return jsonify({"status": "success", "message": "تم تحديث الأهداف"})


@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
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

    latest_stats = session.get("latest_stats")
    if latest_stats:
        return jsonify(latest_stats), 200

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
    data = request.json or {}
    session = load_session_data()
    account_id = session.get("account_id")
    if not account_id:
        return jsonify({"status": "error", "message": "لا يوجد حساب"}), 400

    lockout_hours = float(data.get('lockout_hours', 1))
    reason = data.get('reason', 'غير محدد')

    async def close_all():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.get_account(account_id)
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        positions = await connection.get_positions()
        for pos in positions:
            await connection.close_position(pos['id'])
        
        session["is_locked"] = True
        session["lock_until"] = (datetime.now(timezone.utc) + timedelta(hours=lockout_hours)).isoformat()
        session["profit_target"] = 0
        session["max_loss"] = 0
        save_session_data(session)
        
        return {"status": "success", "message": f"تم الإغلاق: {reason}"}

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
    print("🟢 Trading Guardian - MetaApi Cloud")
    print("تأكد من تعيين METAAPI_TOKEN")
    app.run(host='0.0.0.0', port=5000, debug=False)
