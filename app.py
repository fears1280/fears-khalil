"""
🛡️ Trading Guardian - السيرفر النقي والنهائي 🛡️
(بدون أي ترقيعات أو خدع برمجية)
"""

import os
import sys
import asyncio
import threading
import time
import traceback
import logging
from datetime import datetime
from typing import Dict, Any

from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

# إعدادات الـ Logging لطباعة الأخطاء بوضوح في Render
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type"]}})

API_TOKEN = os.environ.get('METAAPI_TOKEN', '').strip()
if not API_TOKEN:
    logger.error("❌ CRITICAL: METAAPI_TOKEN not found! Check Render Dashboard.")

SESSIONS: Dict[str, Dict[str, Any]] = {}
SESSIONS_LOCK = threading.Lock()
MONITORING_THREADS: Dict[str, threading.Thread] = {}

# ===============================
# دالة الاتصال بـ MetaApi
# ===============================
async def connect_to_metaapi(login_int: int, password: str, server: str) -> str:
    logger.info(f"🔗 Attempting connection: {login_int} on {server}")
    api = MetaApi(API_TOKEN)
    account = None
    
    try:
        accounts = await api.metatrader_account_api.get_accounts()
        if accounts:
            for acc in accounts:
                acc_login = acc.login if hasattr(acc, 'login') else acc.get('login')
                if str(acc_login) == str(login_int):
                    account = acc
                    logger.info("✅ Found existing account")
                    break
    except Exception as e:
        logger.warning(f"⚠️ Bypass fetch accounts: {e}")

    if not account:
        logger.info("➕ Creating new account...")
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login_int}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': int(login_int),
            'password': str(password),
            'server': str(server),
            'magic': 999111,
            'keywords': ['trading-guardian']
        })

    account_id = account.id if hasattr(account, 'id') else account.get('id')
    fresh_account = await api.metatrader_account_api.get_account(account_id)
    
    if fresh_account.state != 'DEPLOYED':
        logger.info("🚀 Deploying account...")
        await fresh_account.deploy()
        
    return account_id

# ===============================
# دالة المراقبة (Background Thread)
# ===============================
async def monitor_account(session_id: str, account_id: str, stop_event: threading.Event):
    logger.info(f"🛡️ Monitor started for {session_id}")
    api = MetaApi(API_TOKEN)
    
    while not stop_event.is_set():
        try:
            account = await api.metatrader_account_api.get_account(account_id)
            if account.state != 'DEPLOYED':
                await account.deploy()
                await asyncio.sleep(2)
            
            connection = account.get_rpc_connection()
            if not connection.is_connected:
                await connection.connect()
            
            account_info = await connection.get_account_information()
            balance = float(getattr(account_info, 'balance', 0.0))
            equity = float(getattr(account_info, 'equity', 0.0))
            profit = equity - balance
            
            with SESSIONS_LOCK:
                if session_id in SESSIONS:
                    session = SESSIONS[session_id]
                    session['balance'] = balance
                    session['equity'] = equity
                    session['profit'] = profit
                    
                    if not session.get('is_locked'):
                        if profit >= session.get('daily_target', 500.0) or profit <= session.get('max_loss_limit', -500.0):
                            session['is_locked'] = True
                            try:
                                positions = await connection.get_positions()
                                for pos in positions:
                                    await connection.close_position(pos.id)
                            except:
                                pass
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"❌ Monitor error: {e}")
            await asyncio.sleep(5)

def start_monitoring_thread(session_id: str, account_id: str):
    stop_event = threading.Event()
    def run_monitor():
        asyncio.run(monitor_account(session_id, account_id, stop_event))
    
    thread = threading.Thread(target=run_monitor, daemon=True)
    thread.start()
    MONITORING_THREADS[session_id] = thread

# ===============================
# API Endpoints
# ===============================
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "✅ Server is running"}), 200

@app.route('/api/connect', methods=['POST', 'OPTIONS'])
def connect_account():
    if request.method == 'OPTIONS':
        return '', 204
    
    if not API_TOKEN:
        return jsonify({"status": "error", "message": "METAAPI_TOKEN is missing in Render Dashboard"}), 500
        
    try:
        data = request.get_json(force=True, silent=True) or {}
        login = data.get('login')
        password = data.get('password')
        server = data.get('server')
        
        if not login or not password or not server:
            return jsonify({"status": "error", "message": "Incomplete data"}), 400
            
        login_int = int(login)
        daily_target = float(data.get('daily_target', 500.0))
        max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
        
        # إنشاء Event Loop معزول ونظيف للتسجيل
        account_id = asyncio.run(connect_to_metaapi(login_int, password, server))
        
        session_id = f"session_{login_int}_{int(time.time())}"
        
        with SESSIONS_LOCK:
            SESSIONS[session_id] = {
                'session_id': session_id,
                'account_id': account_id,
                'daily_target': daily_target,
                'max_loss_limit': max_loss_limit,
                'is_locked': False,
                'balance': 0.0,
                'profit': 0.0
            }
            
        start_monitoring_thread(session_id, account_id)
        return jsonify({"status": "success", "session_id": session_id}), 201
        
    except ValueError:
        return jsonify({"status": "error", "message": "Login must be numbers only"}), 400
    except Exception as e:
        logger.error(f"❌ Connection Crash: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
