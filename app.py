import os
import asyncio
from flask import Flask, jsonify, request
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

@app.route('/')
def home():
    return "Service is UP", 200

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))

    # تشغيل غير متزامن
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def process():
        api = MetaApi(API_TOKEN)
        # الوصول المباشر
        account_api = api.metatrader_account_api
        
        # جلب الحسابات
        accounts = await account_api.get_accounts()
        account = next((acc for acc in accounts if str(acc.login) == login), None)
        
        if not account:
            account = await account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': login,
                'password': password,
                'server': server
            })
            
        if account.state != 'DEPLOYED':
            await account.deploy()
            
        await account.wait_connected(timeout_in_seconds=60)
        return account.id

    try:
        acc_id = loop.run_until_complete(process())
        return jsonify({"status": "success", "account_id": acc_id}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        loop.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
