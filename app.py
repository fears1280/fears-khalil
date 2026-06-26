import os
import asyncio
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

# إعداد الـ Logging لرؤية ما يحدث في السيرفر
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# جلب التوكن
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# مخزن الجلسات
sessions = {}

# دالة آمنة لتشغيل الكود غير المتزامن
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "البيانات ناقصة"}), 400

    async def init_connection():
        logger.info(f"جاري الاتصال للحساب: {login}")
        api = MetaApi(API_TOKEN)
        
        # التأكد من استدعاء مدير الحسابات بشكل صحيح
        account_api = api.metatrader_account_api
        
        logger.info("جاري جلب الحسابات من MetaApi...")
        accounts = await account_api.get_accounts()
        
        # البحث عن الحساب
        account = next((acc for acc in accounts if str(acc.login) == login), None)
        
        if not account:
            logger.info("الحساب غير موجود، جاري إنشاؤه...")
            account = await account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': login,
                'password': password,
                'server': server,
                'magic': 999111
            })
            
        if account.state != 'DEPLOYED':
            logger.info(f"جاري نشر الحساب: {account.id}")
            await account.deploy()
            
        logger.info("جاري انتظار الاتصال بالبروكر...")
        await account.wait_connected(timeout_in_seconds=60)
        
        return account.id

    try:
        acc_id = run_async(init_connection())
        sessions[login] = {"account_id": acc_id}
        return jsonify({"status": "success", "account_id": acc_id}), 200
    except Exception as e:
        logger.error(f"خطأ في الاتصال: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "online"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
