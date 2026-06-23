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

@app.route('/api/connect-user', methods=['POST'])
def connect_user():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    platform = data.get('platform', 'mt5')
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "جميع البيانات مطلوبة: login, password, server"}), 400

    async def register():
        # 🔥 نقوم بإنشاء الـ Api هنا بالداخل ليرتبط بحلقة العمل الحالية بأمان
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
        return {"status": "success", "accountId": account.id}

    try:
        result = run_async(register())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/emergency-kill', methods=['POST'])
def emergency_kill():
    data = request.json or {}
    account_id = data.get('accountId')
    
    if not account_id:
        return jsonify({"status": "error", "message": "Missing accountId"}), 400

    async def close_all():
        # 🔥 إنشاء الكائن داخل دالة الإغلاق أيضاً لحمايتها
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

if __name__ == '__main__':
    app.run()
