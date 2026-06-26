import os
import asyncio
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# مسار الصفحة الرئيسية (مهم جداً لـ Render)
@app.route('/', methods=['GET'])
def home():
    return "The Trading Guardian is UP and RUNNING", 200

# مسار الاتصال
@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات ناقصة"}), 400

    async def init_connection():
        api = MetaApi(API_TOKEN)
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
        # تشغيل الـ Async داخل Context سيكرون
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        acc_id = loop.run_until_complete(init_connection())
        loop.close()
        
        return jsonify({"status": "success", "account_id": acc_id}), 200
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
