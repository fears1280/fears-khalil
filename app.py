from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta

app = Flask(__name__)
SESSION_FILE = "guardian_session.json"

# 🧠 الذاكرة الحية للسيرفر لمنع تصادم الملفات وتصفير القراءات
SHARED_SESSION = {}

def save_session_to_disk(data):
    global SHARED_SESSION
    SHARED_SESSION = data
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)

def load_session_from_disk():
    global SHARED_SESSION
    if SHARED_SESSION:
        return SHARED_SESSION
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                SHARED_SESSION = json.load(f)
                return SHARED_SESSION
            except:
                return {}
    return {}

async def close_all_account_positions(connection):
    try:
        positions = await connection.get_positions()
        for pos in positions:
            action = 'SELL' if pos['type'] == 'POSITION_TYPE_BUY' else 'BUY'
            try:
                await connection.create_market_order(pos['symbol'], action, pos['volume'], {'positionId': pos['id']})
            except Exception as e:
                print(f"💥 Error closing order: {e}")
    except Exception as e:
        print(f"💥 Error fetching open positions: {e}")

# ---------------------------------------------------------------------------
# 🛡️ الحارس التلقائي الذكي
# ---------------------------------------------------------------------------
def start_risk_monitor():
    def monitor_worker():
        async def check_risk_loop():
            while True:
                try:
                    session = load_session_from_disk()
                    account_id = session.get("account_id")
                    is_locked = session.get("is_locked", False)
                    
                    if account_id:
                        if is_locked:
                            lock_until_str = session.get("lock_until")
                            if lock_until_str:
                                if datetime.utcnow() >= datetime.fromisoformat(lock_until_str):
                                    token = os.getenv("METAAPI_TOKEN")
                                    api = MetaApi(token)
                                    account = await api.metatrader_account_api.get_account(account_id)
                                    await account.deploy()
                                    session["is_locked"] = False
                                    session["lock_until"] = None
                                    save_session_to_disk(session)
                            await asyncio.sleep(4)
                            continue

                        token = os.getenv("METAAPI_TOKEN")
                        api = MetaApi(token)
                        account = await api.metavar_account_api.get_account(account_id) if hasattr(api, 'metavar_account_api') else await api.metatrader_account_api.get_account(account_id)
                        
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
                                for deal in deals:
                                    daily_history_profit += (float(deal.get('profit', 0.0)) + float(deal.get('commission', 0.0)) + float(deal.get('swap', 0.0)))
                                if (balance - daily_history_profit) > 0:
                                    overall_growth = (daily_history_profit / (balance - daily_history_profit)) * 100
                            except:
                                pass

                            total_daily_pnl = daily_history_profit + floating_pnl
                            
                            session["latest_stats"] = {
                                "session_valid": True,
                                "is_locked": False,
                                "balance": balance,
                                "equity": equity,
                                "total_progress_drawdown": max(0.0, float(drawdown)),
                                "current_pnl": float(total_daily_pnl), 
                                "daily_profit": float(daily_history_profit),
                                "open_trades": open_trades,
                                "remaining_trades": remaining_trades,
                                "overall_growth": float(overall_growth)
                            }
                            save_session_to_disk(session)
                            
                            profit_target = float(session.get("profit_target", 0.0))
                            max_loss = float(session.get("max_loss", 0.0))
                            lock_minutes = int(session.get("lock_duration_minutes", 120))
                            
                            if (profit_target > 0 and total_daily_pnl >= profit_target) or (max_loss > 0 and total_daily_pnl <= -abs(max_loss)):
                                await close_all_account_positions(connection)
                                await asyncio.sleep(2)
                                session["is_locked"] = True
                                session["lock_until"] = (datetime.utcnow() + timedelta(minutes=lock_minutes)).isoformat()
                                if session.get("latest_stats"):
                                    session["latest_stats"]["is_locked"] = True
                                save_session_to_disk(session)
                                await account.undeploy()
                except:
                    pass
                await asyncio.sleep(4)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

start_risk_monitor()

@app.route('/api/connect', methods=['POST'])
def connect_user():
    data = request.json or {}
    login, password, server = data.get('login'), data.get('password'), data.get('server')
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات ناقصة"}), 400

    async def register():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login}', 'type': 'cloud', 'platform': 'mt5',
            'login': str(login), 'password': password, 'server': server,
            'magic': 999111, 'keywords': ['trading-guardian']
        })
        await account.deploy()
        try:
            await account.wait_connected(timeout_in_seconds=25)
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            account_info = await connection.get_account_information()
            session = {
                "account_id": account.id, "is_locked": False, "profit_target": 500.0, "max_loss": 300.0, "lock_duration_minutes": 120,
                "latest_stats": {
                    "session_valid": True, "is_locked": False, "balance": float(account_info.get('balance', 0.0)),
                    "equity": float(account_info.get('equity', 0.0)), "total_progress_drawdown": 0.0, "current_pnl": 0.0,
                    "daily_profit": 0.0, "open_trades": 0, "remaining_trades": 4, "overall_growth": 0.0
                }
            }
            save_session_to_disk(session)
            return {"status": "success"}
        except:
            await account.cleanup()
            return {"status": "error"}

    try:
        res = run_async(register())
        return jsonify(res), (200 if res["status"] == "success" else 401)
    except Exception as e: 
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    session = load_session_from_disk()
    if not session.get("account_id"):
        return jsonify({"session_valid": False}), 200
        
    latest_stats = session.get("latest_stats")
    if latest_stats:
        latest_stats["is_locked"] = session.get("is_locked", False)
        latest_stats["session_valid"] = True
        return jsonify(latest_stats), 200
        
    return jsonify({"session_valid": False}), 200

@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    session = load_session_from_disk()
    if not session.get("account_id"): return jsonify({"status": "error"}), 400
    session["is_locked"] = True
    session["lock_until"] = (datetime.utcnow() + timedelta(hours=int(request.json.get('lockout_hours', 2)))).isoformat()
    if session.get("latest_stats"): session["latest_stats"]["is_locked"] = True
    save_session_to_disk(session)
    return jsonify({"status": "success"})

@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    data = request.json or {}
    session = load_session_from_disk()
    if 'daily_profit_target' in data: session["profit_target"] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data: session["max_loss"] = float(data['daily_stop_loss'])
    if 'lockout_hours' in data: session["lock_duration_minutes"] = int(data['lockout_hours']) * 60
    save_session_to_disk(session)
    return jsonify({"status": "success"})

@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    global SHARED_SESSION
    SHARED_SESSION = {}
    if os.path.exists(SESSION_FILE): os.remove(SESSION_FILE)
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
