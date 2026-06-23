from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os

app = Flask(__name__)

# دالة مساعدة لإنشاء وإدارة حلقة عمل (Event Loop) مستقلة ونظيفة لكل طلب
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ---------------------------------------------------------------------------
# 1. مسار ربط المستخدم (متوافق مع Flutter)
# ---------------------------------------------------------------------------
@app.route('/api/connect', methods=['POST'])
def connect_user():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    platform = data.get('platform', 'mt5')
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "جميع البيانات مطلوبة: login, password, server"}), 400

    async def register():
        token = os.getenv("METAAPI_TOKEN")
        if not token:
            return {"status": "error", "message": "METAAPI_TOKEN is missing on Render settings"}
            
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
        
        # حفظ الـ Account ID بشكل مؤقت في البيئة لاستخدامه في جلب الإحصائيات
        os.environ["CURRENT_META_ACCOUNT_ID"] = account.id
        return {"status": "success", "accountId": account.id}

    try:
        result = run_async(register())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# 2. مسار جلب الإحصائيات والبيانات الحية (المسار المفقود الذي سبب الـ 404)
# ---------------------------------------------------------------------------
@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    account_id = os.getenv("CURRENT_META_ACCOUNT_ID")
    
    # إذا لم يربط المستخدم حسابه بعد، نرسل بيانات افتراضية سليمة لتجنب انهيار التطبيق
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
        
        # التأكد من جلب حالة الاتصال
        if account.state != 'DEPLOYED':
            await account.deploy()
            
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        # سحب معلومات الحساب الأساسية من MetaApi حياً
        account_information = await connection.get_account_information()
        positions = await connection.get_positions()
        
        balance = account_information.get('balance', 0.0)
        equity = account_information.get('equity', 0.0)
        
        # حساب التراجع ونسب الأرباح
        drawdown = ((balance - equity) / balance * 100) if balance > 0 else 0.0
        
        return {
            "is_locked": False,
            "balance": balance,
            "equity": equity,
            "total_progress_drawdown": max(0.0, drawdown),
            "daily_profit": equity - balance, # الربح أو الخسارة العائمة حالياً
            "remaining_trades": max(0, 4 - len(positions))
        }

    try:
        result = run_async(fetch_stats())
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# 3. مسار الإغلاق الطارئ وتصفية الحساب (متوافق مع Flutter)
# ---------------------------------------------------------------------------
@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    account_id = os.getenv("CURRENT_META_ACCOUNT_ID")
    if not account_id:
        return jsonify({"status": "error", "message": "لم يتم العثور على حساب نشط لتصفيته"}), 400

    async def close_all():
        token = os.getenv("METAAPI_TOKEN")
        api = MetaApi(token)
        
        account = await api.metatrader_account_api.get_account(account_id)
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        await connection.close_all_positions()
        return {"status": "success", "message": "تمت تصفية كافة الصفقات بنجاح للحماية"}

    try:
        result = run_async(close_all())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# 4. مسار فصل الحساب وتصفير الجلسة
# ---------------------------------------------------------------------------
@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    if "CURRENT_META_ACCOUNT_ID" in os.environ:
        del os.environ["CURRENT_META_ACCOUNT_ID"]
    return jsonify({"status": "success", "message": "Disconnected successfully"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
