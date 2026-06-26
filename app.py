import os
import asyncio
from flask import Flask, jsonify, request
from metaapi_cloud_sdk import MetaApi
from upstash_redis import Redis

app = Flask(__name__)

# إعداد Upstash
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))

    # 1. حاول الحصول على الـ ID من Upstash أولاً (تجاوزاً لأي مشكلة في المكتبة)
    cached_id = redis.get(f"acc:{login}")
    if cached_id:
        return jsonify({"status": "success", "account_id": cached_id, "source": "cache"}), 200

    # 2. إذا لم يكن موجوداً، قم بالاتصال (هنا قد يظهر الخطأ إذا لم تُحل مشكلة المكتبة)
    async def init_connection():
        api = MetaApi(API_TOKEN)
        account_api = api.metatrader_account_api
        accounts = await account_api.get_accounts()
        account = next((acc for acc in accounts if str(acc.login) == login), None)
        
        if not account:
            account = await account_api.create_account({...})
            
        if account.state != 'DEPLOYED':
            await account.deploy()
        
        await account.wait_connected(timeout_in_seconds=60)
        
        # تخزين في Upstash لمدة 24 ساعة
        redis.set(f"acc:{login}", account.id, ex=86400)
        return account.id

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        acc_id = loop.run_until_complete(init_connection())
        return jsonify({"status": "success", "account_id": acc_id, "source": "api"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
