import os
import asyncio
import logging
import os
import asyncio
from flask import Flask, jsonify, request
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

@app.route('/api/debug-check', methods=['GET'])
def debug():
    try:
        api = MetaApi(API_TOKEN)
        acc_api = api.metatrader_account_api
        # هذا السطر للتشخيص: يطبع لنا ما هي الدوال المتاحة
        methods = [m for m in dir(acc_api) if not m.startswith('_')]
        return jsonify({"available_methods": methods})
    except Exception as e:
        return jsonify({"error": str(e)})

# ... باقي الكود الخاص بك ...
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# دالة لتشغيل المهام غير المتزامنة بأمان
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')

    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات ناقصة"}), 400

    async def init_account():
        api = MetaApi(API_TOKEN)
        # هنا نستدعي get_accounts من المكتبة المحدثة
        accounts = await api.metatrader_account_api.get_accounts()
        
        # البحث عن الحساب
        account = next((acc for acc in accounts if str(acc.login) == str(login)), None)
        
        if not account:
            logger.info("إنشاء حساب جديد...")
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': str(password),
                'server': str(server)
            })
        
        if account.state != 'DEPLOYED':
            await account.deploy()
            
        await account.wait_connected()
        return account.id

    try:
        acc_id = run_async(init_account())
        return jsonify({"status": "success", "account_id": acc_id}), 200
    except Exception as e:
        logger.error(f"خطأ: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "online"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
