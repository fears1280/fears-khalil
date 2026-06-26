import os
import asyncio
import json
import logging
import time
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

# إعداد الـ Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

API_TOKEN = os.environ.get('METAAPI_TOKEN', '')
# إعداد مخزن الجلسات (بسيط للتطوير)
sessions = {}

# دالة مساعدة لتشغيل الكود غير المتزامن بأمان داخل خيوط Flask
def run_async(coro):
    """تشغيل coroutine في حلقة أحداث جديدة لضمان سلامة الخيوط"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ---------------------------------------------------------------------------
# منطق الاتصال بـ MetaApi
# ---------------------------------------------------------------------------
async def connect_to_account(login, password, server, daily_target, max_loss_limit):
    api = MetaApi(API_TOKEN)
    
    # 1. البحث عن الحساب أو إنشائه
    accounts = await api.metatrader_account_api.get_accounts()
    account = next((acc for acc in accounts if str(acc.login) == str(login)), None)
    
    if not account:
        logger.info(f"✨ إنشاء حساب جديد لـ {login}")
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': str(login),
            'password': str(password),
            'server': str(server),
            'magic': 999111
        })
    
    # 2. النشر (Deploy) والانتظار
    if account.state != 'DEPLOYED':
        logger.info(f"🚀 نشر الحساب: {account.id}")
        await account.deploy()
    
    # 3. الانتظار الصارم للاتصال (هذا يحل مشكلة الـ 500 Timeout)
    logger.info("⏳ انتظار اتصال البروكر...")
    await account.wait_connected(timeout_in_seconds=60)
    
    # 4. الاتصال بـ RPC
    connection = account.get_rpc_connection()
    await connection.connect()
    await connection.wait_synchronized()
    
    return account, connection

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    try:
        account, connection = run_async(connect_to_account(
            data['login'], data['password'], data['server'],
            float(data.get('daily_target', 500)), float(data.get('max_loss_limit', -500))
        ))
        
        session_id = f"session_{data['login']}"
        sessions[session_id] = {
            'account_id': account.id,
            'is_locked': False
        }
        
        return jsonify({"status": "success", "session_id": session_id}), 200
    except Exception as e:
        logger.error(f"❌ Connection error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/account-stats', methods=['GET'])
def get_stats():
    session_id = request.args.get('session_id')
    if session_id not in sessions:
        return jsonify({"status": "error", "message": "Invalid session"}), 401
        
    async def fetch():
        api = MetaApi(API_TOKEN)
        account = await api.metatrader_account_api.get_account(sessions[session_id]['account_id'])
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        return await connection.get_account_information()

    try:
        info = run_async(fetch())
        return jsonify({"status": "success", "balance": info.get('balance')}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "online"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
