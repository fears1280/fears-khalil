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

# 🛡️ دالة مساعدة لإغلاق كافة الصفقات بشكل صحيح عبر أوامر عكسية في MetaApi
async def close_all_account_positions(connection):
    try:
        positions = await connection.get_positions()
        print(f"📦 جاري إغلاق صفقات نشطة عددها: {len(positions)}")
        for pos in positions:
            action = 'SELL' if pos['type'] == 'POSITION_TYPE_BUY' else 'BUY'
            try:
                await connection.create_market_order(
                    pos['symbol'], 
                    action, 
                    pos['volume'], 
                    {'positionId': pos['id']}
                )
                print(f"✅ تم إغلاق الصفقة رقم: {pos['id']}")
            except Exception as order_err:
                print(f"❌ خطأ في إغلاق الصفقة {pos['id']}: {order_err}")
    except Exception as e:
        print(f"❌ خطأ أثناء جلب الصفقات للإغلاق: {e}")

# ---------------------------------------------------------------------------
# 🔐 الحارس الخلفي للمراقبة التلقائية
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
                        if is_locked:
                            lock_until_str = session.get("lock_until")
                            if lock_until_str:
                                lock_until = datetime.fromisoformat(lock_until_str)
                                if datetime.utcnow() >= lock_until:
                                    print("🔓 انتهت مدة القفل التلقائي! إعادة التفعيل...")
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
                            floating_pnl = equity - balance
                            
                            drawdown = ((balance - equity) / balance * 100) if balance > equity else 0.0
                            open_trades = len(positions)
                            remaining_trades = max(0, 4 - open_trades)
                            
                            daily_history_profit = 0.0
                            overall_growth = 0.0
                            
                            try:
                                now = datetime.utcnow()
                                start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
                                deals = await connection.get_deals_by_time_range(start_of_today, now)
                                if deals:
                                    for deal in deals:
                                        profit = float(deal.get('profit', 0.0))
                                        commission = float(deal.get('commission', 0.0))
                                        swap = float(deal.get('swap', 0.0))
                                        daily_history_profit += (profit + commission + swap)
                                    
                                    starting_balance = balance - daily_history_profit
                                    if starting_balance > 0:
                                        overall_growth = (daily_history_profit / starting_balance) * 100
                            except Exception as history_error:
                                print(f"⚠️ فشل جلب الهيستوري: {history_error}")

                            total_daily_pnl = daily_history_profit + floating_pnl
                            
                            # تحديث الكاش مع مطابقة اسم مفتاح التراجع مع الفلوتر (total_progress_drawdown)
                            session["latest_stats"] = {
                                "is_locked": False,
                                "balance": balance,
                                "equity": equity,
                                "total_progress_drawdown": max(0.0, float(drawdown)),
                                "current_pnl": float(floating_pnl),
                                "daily_profit": float(daily_history_profit),
                                "open_trades": open_trades,
                                "remaining_trades": remaining_trades,
                                "overall_growth": float(overall_growth)
                            }
                            save_session_data(session)
                            
                            profit_target = float(session.get("profit_target", 0.0))
                            max_loss = float(session.get("max_loss", 0.0))
                            lock_minutes = int(session.get("lock_duration_minutes", 120))
                            
                            trigger_lock = False
                            if profit_target > 0 and total_daily_pnl >= profit_target:
                                trigger_lock = True
                            if max_loss > 0 and total_daily_pnl <= -abs(max_loss):
                                trigger_lock = True
                            
                            if trigger_lock:
                                print("🛑 تم رصد اختراق الأهداف من الحارس التلقائي! جاري التصفية...")
                                await close_all_account_positions(connection)
                                await asyncio.sleep(3) # مهلة تأكيد الإغلاق في السيرفر قبل الـ undeploy
                                
                                lock_until_time = datetime.utcnow() + timedelta(minutes=lock_minutes)
                                session["is_locked"] = True
                                session["lock_until"] = lock_until_time.isoformat()
                                session["latest_stats"]["is_locked"] = True
                                save_session_data(session)
                                
                                await account.undeploy()
                                print(f"🔒 تم قفل الحساب بنجاح.")
                                
                except Exception as e:
                    print(f"❌ خطأ الحارس العام: {e}")
                await asyncio.sleep(3)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

start_risk_monitor()

# ---------------------------------------------------------------------------
# 🚀 الروابط البرمجية (Endpoints) متوافقة 100% مع الفلوتر
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

    async def register_and_validate():
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
        
        try:
            await account.wait_connected(timeout_in_seconds=30)
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            account_info = await connection.get_account_information()
            
            session = {
                "account_id": account.id,
                "is_locked": False,
                "profit_target": 500.0,
                "max_loss": 300.0,
                "lock_duration_minutes": 120,
                "latest_stats": {
                    "is_locked": False,
                    "balance": float(account_info.get('balance', 0.0)),
                    "equity": float(account_info.get('equity', 0.0)),
                    "total_progress_drawdown": 0.0,
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
            await account.cleanup()
            return {"status": "error", "message": "فشل الاتصال بالبروكر"}

    try:
        result = run_async(register_and_validate())
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 401
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
            "total_progress_drawdown": 0.0,
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
        "total_progress_drawdown": 0.0,
        "current_pnl": 0.0,
        "daily_profit": 0.0,
        "open_trades": 0,
        "remaining_trades": 4,
        "overall_growth": 0.0
    }), 200


# ⚡ رابط الإغلاق الطارئ المطلوب من الفلوتر (تم ربطه بالكامل)
@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    data = request.json or {}
    session = load_session_data()
    account_id = session.get("account_id")
    
    if not account_id:
        return jsonify({"status": "error", "message": "لا يوجد حساب نشط"}), 400
        
    lockout_hours = int(data.get('lockout_hours', 2))
    
    # حجز حالة القفل فوراً لمنع التكرار
    session["is_locked"] = True
    session["lock_until"] = (datetime.utcnow() + timedelta(hours=lockout_hours)).isoformat()
    save_session_data(session)
    
    # تنفيذ مهمة الإغلاق الفعلي على خيط منفصل لتجنب تعليق الطلب
    def execute_closure():
        async def close_tasks():
            try:
                token = os.getenv("METAAPI_TOKEN")
                api = MetaApi(token)
                account = await api.metatrader_account_api.get_account(account_id)
                if account.state == 'DEPLOYED':
                    connection = account.get_rpc_connection()
                    await connection.connect()
                    await connection.wait_synchronized()
                    
                    # إغلاق الصفقات
                    await close_all_account_positions(connection)
                    await asyncio.sleep(3) # مهلة للبروكر
                    
                    # فصل السيرفر تماماً
                    await account.undeploy()
                    print("🔒 تم تفعيل الإغلاق العاجل وفصل الحساب من رابط الطوارئ.")
            except Exception as ex:
                print(f"❌ خطأ تنفيذ الطوارئ: {ex}")
                
        run_async(close_tasks())

    threading.Thread(target=execute_closure, daemon=True).start()
    return jsonify({"status": "success", "message": "تم استقبال أمر الطوارئ وجاري التنفيذ"})


# ⚙️ رابط تحديث الإعدادات المطلوب من الفلوتر (تم ربطه بالكامل)
@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    data = request.json or {}
    session = load_session_data()
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب نشط"}), 400
        
    if 'daily_profit_target' in data:
        session["profit_target"] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        session["max_loss"] = float(data['daily_stop_loss'])
    if 'lockout_hours' in data:
        session["lock_duration_minutes"] = int(data['lockout_hours']) * 60
        
    save_session_data(session)
    return jsonify({"status": "success", "message": "تم تحديث الإعدادات والأهداف بنجاح"})


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return jsonify({"status": "success", "message": "Disconnected"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
