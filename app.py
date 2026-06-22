from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os

app = Flask(__name__)

# جلب التوكن من بيئة السيرفر الآمنة (وليس كتابته كصيغة نصية في الكود)
API_TOKEN = os.getenv("METAAPI_TOKEN", "ضع_التوكن_الخاص_بك_هنا_للتجربة_المحلية")
api = MetaApi(API_TOKEN)

@app.route('/api/connect-user', methods=['POST'])
def connect_user():
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "لم يتم إرسال بيانات"}), 400

    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    platform = data.get('platform', 'mt5')

    async def register_on_cloud():
        try:
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
        except Exception as e:
            return {"status": "error", "message": str(e)}

    result = asyncio.run(register_on_cloud())
    return jsonify(result)

@app.route('/api/emergency-kill', methods=['POST'])
def emergency_kill():
    data = request.json
    account_id = data.get('accountId')
    
    if not account_id:
        return jsonify({"status": "error", "message": "Missing accountId"}), 400

    async def close_all():
        try:
            account = await api.metatrader_account_api.get_account(account_id)
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            await connection.close_all_positions()
            return {"status": "success", "message": "تمت تصفية كافة الصفقات بنجاح"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    result = asyncio.run(close_all())
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
