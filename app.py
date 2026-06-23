from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

SESSION_FILE = "guardian_session.json"
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")   # تأكد من تعيينه في متغيرات البيئة

def run_async(coro):
    """تشغيل كوروتين asyncio في خيط منفصل بأمان"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def load_session():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_session(data):
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ============================================================
# 🛡️ حلقة المراقبة الذكية (تعمل في الخلفية)
# ============================================================
def start_guardian_loop():
    def worker():
        async def guardian():
            while True:
                try:
                    session = load_session()
                    account_id = session.get("account_id")
                    if not account_id:
                        await asyncio.sleep(2)
                        continue

                    is_locked = session.get("is_locked", False)
                    lock_until_str = session.get("lock_until")

                    # 1️⃣ فحص القفل الزمني: إذا انتهت المدة نعيد الحساب تلقائياً
                    if is_locked and lock_until_str:
                        lock_until = datetime.fromisoformat(lock_until_str)
                        if datetime.now(timezone.utc) >= lock_until:
                            print("🔓 انتهت مدة القفل – جاري إعادة تشغيل الحساب...")
                            api = MetaApi(METAAPI_TOKEN)
                            account = await api.metatrader_account_api.get_account(account_id)
                            await account.deploy()
                            await account.wait_connected()
                            session["is_locked"] = False
                            session["lock_until"] = None
                            session["latest_stats"] = None
                            save_session(session)
                        await asyncio.sleep(3)
                        continue

                    if is_locked:
                        await asyncio.sleep(2)
                        continue

                    # 2️⃣ جلب البيانات الحية
                    api = MetaApi(METAAPI_TOKEN)
                    account = await api.metatrader_account_api.get_account(account_id)
                    if account.state != 'DEPLOYED':
                        await account.deploy()
                        await account.wait_connected()

                    connection = account.get_rpc_connection()
                    await connection.connect()
                    await connection.wait_synchronized()

                    info = await connection.get_account_information()
                    positions = await connection.get_positions()

                    balance = float(info.get('balance', 0.0))
                    equity = float(info.get('equity', 0.0))
                    current_pnl = equity - balance
                    open_trades = len(positions)

                    # ✅ الحسابات المئوية
                    drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
                    daily_profit = current_pnl   # يمكن تطويره لاحقاً لحساب أرباح اليوم من الهيستوري
                    overall_growth = (daily_profit / balance * 100) if balance > 0 else 0.0

                    # تحديث الكاش
                    session["latest_stats"] = {
                        "is_locked": False,
                        "balance": balance,
                        "equity": equity,
                        "current_pnl": current_pnl,
                        "drawdown_percent": round(max(0.0, drawdown), 2),
                        "daily_profit": round(daily_profit, 2),
                        "overall_growth": round(overall_growth, 2),
                        "open_trades": open_trades,
                        "remaining_trades": max(0, 4 - open_trades)
                    }
                    save_session(session)

                    # 3️⃣ فحص الأهداف
                    profit_target = float(session.get("profit_target", 0.0))
                    max_loss = float(session.get("max_loss", 0.0))
                    lockout_hours = float(session.get("lockout_hours", 1.0))

                    trigger_lock = False
                    if profit_target > 0 and current_pnl >= profit_target:
                        trigger_lock = True
                        print(f"🎯 تم تحقيق الهدف: {current_pnl}$")
                    elif max_loss > 0 and current_pnl <= -abs(max_loss):
                        trigger_lock = True
                        print(f"🛑 تم تجاوز حد الخسارة: {current_pnl}$")

                    if trigger_lock:
                        # إغلاق جميع الصفقات
                        for pos in positions:
                            try:
                                await connection.close_position(pos['id'])
                                print(f"✓ أغلقت الصفقة {pos['id']}")
                            except Exception as e:
                                print(f"⚠️ تعذر إغلاق {pos['id']}: {e}")

                        # تفعيل القفل وإيقاف الحساب
                        lock_until = datetime.now(timezone.utc) + timedelta(hours=lockout_hours)
                        session["is_locked"] = True
                        session["lock_until"] = lock_until.isoformat()
                        session["latest_stats"] = {
                            "is_locked": True,
                            "balance": balance,
                            "equity": equity,
                            "current_pnl": 0.0,
                            "drawdown_percent": 0.0,
                            "daily_profit": 0.0,
                            "overall_growth": 0.0,
                            "open_trades": 0,
                            "remaining_trades": 0
                        }
                        save_session(session)

                        await account.undeploy()   # فصل الحساب تماماً
                        print(f"🔒 الحساب مقفل حتى {lock_until}")

                except Exception as e:
                    print(f"❌ خطأ في الحلقة: {e}")
                await asyncio.sleep(2)   # تحديث كل ثانيتين

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(guardian())

    threading.Thread(target=worker, daemon=True).start()

# تشغيل الحلقة عند بدء الخادم
start_guardian_loop()

# ============================================================
# 🚀 نقاط API
# ============================================================

@app.route('/api/connect', methods=['POST'])
def connect_account():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')

    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "البيانات ناقصة"}), 400

    async def create():
        api = MetaApi(METAAPI_TOKEN)
        # إنشاء حساب جديد على السحابة
        acc = await api.metatrader_account_api.create_account({
            'name': f'Guardian-{login}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': str(login),
            'password': password,
            'server': server,
        })
        await acc.deploy()
        await acc.wait_connected()

        session = load_session()
        session["account_id"] = acc.id
        session["is_locked"] = False
        session["lock_until"] = None
        session["latest_stats"] = None
        save_session(session)
        return {"status": "success", "account_id": acc.id}

    try:
        result = run_async(create())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    """استقبال الأهداف من التطبيق (daily_profit_target, daily_stop_loss, lockout_hours)"""
    data = request.json or {}
    session = load_session()

    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب متصل"}), 400

    # دعم أسماء الحقول المرسلة من التطبيق
    if 'daily_profit_target' in data:
        session['profit_target'] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        session['max_loss'] = float(data['daily_stop_loss'])
    if 'lockout_hours' in data:
        session['lockout_hours'] = float(data['lockout_hours'])

    save_session(session)
    return jsonify({"status": "success", "targets": {
        "profit_target": session.get('profit_target', 0),
        "max_loss": session.get('max_loss', 0),
        "lockout_hours": session.get('lockout_hours', 1)
    }})

@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    """إرجاع أحدث بيانات الكاش مباشرة"""
    session = load_session()
    stats = session.get("latest_stats")
    if stats:
        return jsonify(stats)
    # بيانات أولية إذا لم تتوفر بعد
    return jsonify({
        "is_locked": session.get("is_locked", False),
        "balance": 0.0,
        "equity": 0.0,
        "current_pnl": 0.0,
        "drawdown_percent": 0.0,
        "daily_profit": 0.0,
        "overall_growth": 0.0,
        "open_trades": 0,
        "remaining_trades": 4
    })

@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    """إغلاق طارئ يدوي من التطبيق (اختياري)"""
    data = request.json or {}
    session = load_session()
    account_id = session.get("account_id")
    if not account_id:
        return jsonify({"status": "error", "message": "لا يوجد حساب"}), 400

    lockout_hours = float(data.get('lockout_hours', 1))

    async def force_close():
        api = MetaApi(METAAPI_TOKEN)
        acc = await api.metatrader_account_api.get_account(account_id)
        if acc.state == 'DEPLOYED':
            conn = acc.get_rpc_connection()
            await conn.connect()
            await conn.wait_synchronized()
            positions = await conn.get_positions()
            for p in positions:
                await conn.close_position(p['id'])

        session["is_locked"] = True
        session["lock_until"] = (datetime.now(timezone.utc) + timedelta(hours=lockout_hours)).isoformat()
        session["latest_stats"]["is_locked"] = True
        save_session(session)
        await acc.undeploy()
        return {"status": "success", "message": "تم الإغلاق الطارئ"}

    try:
        result = run_async(force_close())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return jsonify({"status": "success", "message": "تم حذف الجلسة"})

if __name__ == '__main__':
    print("🟢 Trading Guardian Server – MetaApi Cloud")
    print("تأكد من تعيين METAAPI_TOKEN في متغيرات البيئة")
    app.run(host='0.0.0.0', port=5000, debug=False)
