import os
import asyncio
import logging
import sys
from flask import Flask, request, jsonify
from flask_cors import CORS

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# استيراد المكتبة (بعد التأكد من عدم وجود ملفات محلية بنفس الاسم)
try:
    from metaapi_cloud_sdk import MetaApi
    logger.info("✅ تم استيراد MetaApi بنجاح.")
except Exception as e:
    logger.error(f"❌ فشل استيراد MetaApi: {e}")

app = Flask(__name__)
CORS(app)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# دالة لتشغيل المهام غير المتزامنة
def run_async(coro):
    return asyncio.run(coro)

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))
    
    if not all([login, password, server]):
        return jsonify({"error": "بيانات ناقصة"}), 400

    async def get_account_id():
        api = MetaApi(API_TOKEN)
        # هنا نستخدم الطريقة المباشرة الموثقة
        account_api = api.metatrader_account_api
        
        # تشخيص سريع: إذا كان هناك خطأ، سيعطيك الـ Log التالي تفاصيل المكتبة
        logger.info(f"نوع الـ account_api: {type(account_api)}")
        
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
        acc_id = run_async(get_account_id())
        return jsonify({"status": "success", "account_id": acc_id}), 200
    except Exception as e:
        logger.error(f"خطأ غير متوقع: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
