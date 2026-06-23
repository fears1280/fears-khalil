from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta

app = Flask(__name__)

# ملف محلي لحفظ الجلسة والأهداف بشكل مستقر وآمن بين الـ Workers
SESSION_FILE = "guardian_session.json"

# دالة مساعدة لإدارة حلقة العمل (Event Loop) للطلبات المتزامنة
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# دالة لحفظ بيانات الجلسة والأهداف
def save_session_data(data):
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f)

# دالة لقراءة بيانات الجلسة والأهداف
def load_session_data():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

# ---------------------------------------------------------------------------
# 🔐 نظام المراقبة الخلفي الذكي (Background Risk Manager)
# يعمل كل 3 ثوانٍ بشكل مستقل تماماً لحماية الحساب حتى لو كان تطبيق فلوتر مغلقاً!
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
                        # 1. التحقق من انتهاء مدة القفل لإعادة تفعيل الحساب تلقائياً
                        if is_locked:
                            lock_until_str = session.get("lock_until")
                            if lock_until_str:
                                lock_until = datetime.fromisoformat(lock_until_str)
                                if datetime.utcnow() >= lock_until:
                                    print("🔓 انتهت مدة القفل! إعادة تفعيل حساب الميتا الآن...")
                                    token = os.getenv("METAAPI_TOKEN")
                                    api = MetaApi(token)
                                    account = await api.metatrader_account_api.get_account(account_id)
                                    await account.deploy() # فتح الميتا مجدداً
                                    session["is_locked"] = False
                                    session["lock_until"] = None
                                    save_session_data(session)
                            await asyncio.sleep(5)
                            continue

                        # 2. المراقبة الحية للحساب والأهداف إذا لم يكن مقفلاً
                        token = os.getenv("METAAPI_TOKEN")
                        api = MetaApi(token)
                        account = await api.metatrader_account_api.get_account(account_id)
                        
                        if account.state == 'DEPLOYED':
                            connection = account.get_rpc_connection()
                            await connection.connect()
                            await connection.wait_synchronized()
                            
                            account_info = await connection.get_account_information()
                            balance = account_info.get('balance', 0.0)
                            equity = account_info.get('equity', 0.0)
                            current_pnl = equity - balance # الربح أو الخسارة العائمة الحالية
                            
                            profit_target = float(session.get("profit_target", 0.0))
                            max_loss = float(session.get("max_loss", 0.0))
                            lock_minutes = int(session.get("lock_duration_minutes", 60))
                            
                            trigger_lock = False
                            reason = ""
                            
                            # التحقق من هدف الأرباح
                            if profit_target > 0 and current_pnl >= profit_target:
                                trigger_lock = True
                                reason = f"🎯 تم الوصول لركيزة الربح المستهدف: {current_pnl}$"
                                
                            # التحقق من الحد الأقصى للخسارة
                            if max_loss > 0 and current_pnl <= -abs(max_loss):
                                trigger_lock = True
                                reason = f"🛑 تم ضرب الحد الأقصى للخسارة المسموحة: {current_pnl}$"
                            
                            if trigger_lock:
                                print(f"⚠️ {reason} | جاري تنفيذ خطة الطوارئ وتصفية الحساب...")
                                # أ. إغلاق وتصفيات كافة الصفقات فوراً
                                await connection.close_all_positions()
                                
                                # ب. حساب توقيت فتح الحساب مجدداً
                                lock_until_time = datetime.utcnow() + timedelta(minutes=lock_minutes)
                                session["is_locked"] = True
                                session["lock_until"] = lock_until_time.isoformat()
                                save_session_data(session)
                                
                                # ج. إغلاق الميتا تماماً (Undeploy) لمنع التداول نهائياً
                                await account.undeploy()
                                print(f"🔒 تم تصفية الحساب وإغلاق الميتا بنجاح. سيعود للعمل في: {lock_until_time}")
                                
                except Exception as e:
                    print(f"❌ خطأ في سيرفر المراقبة: {e}")
                await asyncio.sleep(3) # فحص شامل وحي كل 3 ثوانٍ

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(check_risk_loop())

    t = threading.Thread(target=monitor_worker, daemon=True)
    t.start()

# تشغيل نظام المراقبة الذكي فور إقلاع السيرفر
start_risk_monitor()

# ---------------------------------------------------------------------------
# 🚀 الروابط (Endpoints) المتوافقة بالكامل مع تطبيق Flutter
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
        if not token:
            return {"status": "error", "message": "METAAPI_TOKEN is missing"}
            
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
        
        # إنشاء الجلسة وحفظ الـ ID مع قيم افتراضية للأهداف
        session = load_session_data()
        session["account_id"] = account.id
        session["is_locked"] = False
        save_session_data(session)
        
        return {"status": "success", "accountId": account.id}

    try:
        result = run_async(register())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/set-targets', methods=['POST'])
def set_targets():
    """ رابط مخصص لتلقي الأهداف ومدة القفل مباشرة من واجهة الفلوتر """
    data = request.json or {}
    profit_target = data.get('profit_target', 0.0)
    max_loss = data.get('max_loss', 0.0)
    lock_duration_minutes = data.get('lock_duration_minutes', 60) # بالدقائق
    
    session = load_session_data()
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب نشط لتعيين الأهداف له"}), 400
        
    session["profit_target"] = float(profit_target)
    session["max_loss"] = float(max_loss)
    session["lock_duration_minutes"] = int(lock_duration_minutes)
    save_session_data(session)
    
    return jsonify({"status": "success", "message": "تم تحديث الأهداف ونظام الحماية بنجاح"})


@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    session = load_session_data()
    account_id = session.get("account_id")
    is_locked = session.get("is_locked", False)
    
    # إذا كان الحساب في حالة قفل (بسبب ضرب الأهداف) نبلغ الفلوتر فوراً ليقفل الواجهة
    if is_locked:
        return jsonify({
            "is_locked": True,
            "balance": 0.0,
            "equity": 0.0,
            "total_progress_drawdown": 0.0,
            "daily_profit": 0.0,
            "remaining_trades": 0
        }), 200

    if not account_id:
        return jsonify({
            "is_locked": False,
            "balance": 0.0,
            "equity": 0.0,
            "total_progress_drawdown": 0.0,
            "daily_profit": 0.0,
            "remaining_trades": 4
        }), 200

    async def fetch_stats():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.get_account(account_id)
        
        if account.state != 'DEPLOYED':
            return {
                "is_locked": False,
                "balance": 0.0,
                "equity": 0.0,
                "total_progress_drawdown": 0.0,
                "daily_profit": 0.0,
                "remaining_trades": 4
            }
            
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        account_information = await connection.get_account_information()
        positions = await connection.get_positions()
        
        balance = account_information.get('balance', 0.0)
        equity = account_information.get('equity', 0.0)
        
        # حساب النسبة المئوية للتراجع العائم (Drawdown %)
        drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
        
        return {
            "is_locked": False,
            "balance": float(balance),
            "equity": float(equity),
            "total_progress_drawdown": max(0.0, float(drawdown)),
            "daily_profit": float(equity - balance), # الـ PnL الحالي العائم ليظهر في الخانات الحية
            "remaining_trades": max(0, 4 - len(positions))
        }

    try:
        result = run_async(fetch_stats())
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    session = load_session_data()
    account_id = session.get("account_id")
    if not account_id:
        return jsonify({"status": "error", "message": "لم يتم العثور على حساب نشط"}), 400

    async def close_all():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        account = await api.metatrader_account_api.get_account(account_id)
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        await connection.close_all_positions()
        return {"status": "success", "message": "تمت تصفية كافة الصفقات بنجاح"}

    try:
        result = run_async(close_all())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
    return jsonify({"status": "success", "message": "Disconnected successfully"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
