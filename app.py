import os
import asyncio
import time
import json
import redis
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi
from urllib.parse import urlparse

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# جلب الـ Tokens من متغيرات البيئة في Render
# ---------------------------------------------------------------------------
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# ---------------------------------------------------------------------------
# الاتصال بـ Redis (لحفظ الجلسات بشكل دائم)
# ---------------------------------------------------------------------------
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ---------------------------------------------------------------------------
# دوال مساعدة للتعامل مع Redis
# ---------------------------------------------------------------------------
def save_session(session_id, data):
    """حفظ الجلسة في Redis مع صلاحية ساعتين"""
    redis_client.setex(f"session:{session_id}", 7200, json.dumps(data))

def get_session(session_id):
    """استرجاع الجلسة من Redis"""
    data = redis_client.get(f"session:{session_id}")
    return json.loads(data) if data else None

def delete_session(session_id):
    """حذف الجلسة من Redis"""
    redis_client.delete(f"session:{session_id}")

# ---------------------------------------------------------------------------
# دالة مساعدة للوصول الآمن إلى ID الصفقة (Dict vs Object)
# ---------------------------------------------------------------------------
def safe_get_position_id(pos):
    """استخراج ID الصفقة بغض النظر عن نوعها (Dict أو Object)"""
    if isinstance(pos, dict):
        return pos.get('id')
    return getattr(pos, 'id', None)

# ---------------------------------------------------------------------------
# 1. دالة تسجيل الدخول والربط (Connect) - Async بالكامل
# ---------------------------------------------------------------------------
@app.route('/api/connect', methods=['POST'])
async def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    try:
        daily_target = float(data.get('daily_target', 500.0))
        max_loss_limit = -abs(float(data.get('max_loss_limit', 500.0)))
    except (ValueError, TypeError):
        daily_target = 500.0
        max_loss_limit = -500.0
    
    if not all([login, password, server]):
        return jsonify({"status": "error", "message": "بيانات غير كاملة"}), 400
    
    if not API_TOKEN:
        return jsonify({"status": "error", "message": "METAAPI_TOKEN غير معرف في سيرفر Render"}), 500
    
    try:
        api = MetaApi(API_TOKEN)
        account = None
        
        # محاولة البحث عن حساب موجود مسبقاً
        try:
            print("🔄 Checking existing accounts on MetaApi...")
            existing_accounts = await api.metatrader_account_api.get_accounts()
            
            if existing_accounts and isinstance(existing_accounts, list):
                for acc in existing_accounts:
                    try:
                        acc_login = None
                        if isinstance(acc, dict):
                            acc_login = acc.get('login')
                        else:
                            acc_login = getattr(acc, 'login', None) or (acc.get('login') if hasattr(acc, 'get') else None)
                        
                        if acc_login and str(acc_login) == str(login):
                            account = acc
                            print(f"♻️ Found existing MetaApi account for login: {login}")
                            break
                    except:
                        continue
        except Exception as check_error:
            print(f"⚠️ Safe bypass: Quick check failed ({check_error}), proceeding to registration.")
        
        # إنشاء حساب جديد إذا لم يوجد
        if not account:
            print(f"✨ Creating new MetaApi account for login: {login}")
            account = await api.metatrader_account_api.create_account({
                'name': f'Guardian_{login}',
                'type': 'cloud',
                'platform': 'mt5',
                'login': str(login),
                'password': str(password),
                'server': str(server),
                'magic': 999111,
                'keywords': ['trading-guardian']
            })
        
        # الحصول على معرف الحساب وحالته
        try:
            account_state = account.state if hasattr(account, 'state') else account.get('state', '')
            account_id = account.id if hasattr(account, 'id') else account.get('id')
        except:
            account_state = 'UNKNOWN'
            account_id = getattr(account, 'id', None)
            
        # نشر الحساب إذا لم يكن منشوراً
        if account_state != 'DEPLOYED':
            await account.deploy()
        
        print("⏳ Waiting for account connection setup...")
        await account.wait_connected(timeout_in_seconds=30)
        
        # إنشاء اتصال RPC
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        # جلب معلومات الحساب
        account_info = await connection.get_account_information()
        initial_balance = float(account_info.get('balance', 0.0))
        
        # إنشاء معرف الجلسة
        session_id = f"session_{login}_{int(time.time())}"
        session_data = {
            'session_id': session_id,
            'account_id': account_id,
            'login': str(login),
            'server': str(server),
            'status': 'connected',
            'connected_at': datetime.now().isoformat(),
            'daily_target': daily_target,
            'max_loss_limit': max_loss_limit,
            'is_locked': False,
            'daily_profit': 0.0,
            'daily_loss': 0.0,
            'balance': initial_balance,
            'equity': initial_balance,
            'initial_balance': initial_balance
        }
        
        # حفظ الجلسة في Redis
        save_session(session_id, session_data)
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "account_id": account_id,
            "message": "تم الاتصال وتأمين الحساب بنجاح!"
        }), 201
            
    except Exception as e:
        print(f"Connection API Critical Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 2. دالة الإحصائيات والمراقبة الصارمة (تغلق أي صفقة جديدة لمح لمح البصر)
# ---------------------------------------------------------------------------
@app.route('/api/account-stats', methods=['GET'])
async def account_stats():
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    # جلب الجلسة من Redis
    session = get_session(session_id)
    
    if not session:
        return jsonify({"status": "error", "message": "خطأ في المصادقة: الجلسة منتهية"}), 401

    try:
        api = MetaApi(API_TOKEN)
        account = await api.metatrader_account_api.get_account(session['account_id'])
        
        if account.state != 'DEPLOYED':
            await account.deploy()
            
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        account_info = await connection.get_account_information()
        positions = await connection.get_positions()
        
        balance = float(account_info.get('balance', 0.0))
        equity = float(account_info.get('equity', 0.0))
        pnl = equity - balance
        
        initial = float(session.get('initial_balance', balance))
        drawdown_percent = abs((pnl / balance) * 100) if pnl < 0 and balance > 0 else 0.0
        overall_growth = ((balance - initial) / initial) * 100 if initial > 0 else 0.0
        
        session['balance'] = balance
        session['equity'] = equity

        # 🛑 المراقبة الصارمة: إذا قفلنا الحساب ووجدت صفقات مفتوحة
        if session.get('is_locked'):
            if len(positions) > 0:
                print(f"🚨 LOCKOUT VIOLATION! User opened {len(positions)} trades. Closing them now!")
                for pos in positions:
                    pos_id = safe_get_position_id(pos)
                    if pos_id:
                        try:
                            await connection.close_position(pos_id)
                        except:
                            try:
                                await connection.cancel_order(pos_id)
                            except:
                                pass
                positions = []

        # 📈 الفحص التلقائي للهدف والخسارة
        if not session.get('is_locked'):
            daily_target = session.get('daily_target', 500.0)
            max_loss_limit = session.get('max_loss_limit', -500.0)
            
            if pnl >= daily_target or pnl <= max_loss_limit:
                print(f"🎯 Target or Stop reached on server side! Locking down account.")
                session['is_locked'] = True
                for pos in positions:
                    pos_id = safe_get_position_id(pos)
                    if pos_id:
                        try:
                            await connection.close_position(pos_id)
                        except:
                            pass
                positions = []
                
                # تحديث الجلسة في Redis بعد القفل
                save_session(session_id, session)

        # تحديث الجلسة في Redis مع أحدث البيانات
        save_session(session_id, session)

        return jsonify({
            "status": "success",
            "data": {
                "is_locked": session.get('is_locked', False),
                "balance": balance,
                "equity": equity,
                "current_pnl": 0.0 if session.get('is_locked') else pnl,
                "drawdown_percent": 0.0 if session.get('is_locked') else drawdown_percent,
                "daily_profit": session.get('daily_profit', 0.0),
                "overall_growth": overall_growth,
                "open_trades": len(positions)
            }
        }), 200

    except Exception as e:
        print(f"Stats API Error: {e}")
        return jsonify({
            "status": "success",
            "data": {
                "is_locked": session.get('is_locked', False),
                "balance": session.get('balance', 0.0),
                "equity": session.get('equity', 0.0),
                "current_pnl": 0.0,
                "drawdown_percent": 0.0,
                "daily_profit": session.get('daily_profit', 0.0),
                "overall_growth": 0.0,
                "open_trades": 0
            }
        }), 200

# ---------------------------------------------------------------------------
# 3. دالة تحديث قيم الأهداف (Update Targets) من التطبيق
# ---------------------------------------------------------------------------
@app.route('/api/update-targets', methods=['POST'])
async def update_targets():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    if 'daily_profit_target' in data:
        session['daily_target'] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        session['max_loss_limit'] = -abs(float(data['daily_stop_loss']))
    if 'early_warning' in data:
        session['early_warning'] = bool(data['early_warning'])
        
    # حفظ التحديثات في Redis
    save_session(session_id, session)
        
    return jsonify({"status": "success", "message": "تم تحديث الأهداف بنجاح"}), 200

# ---------------------------------------------------------------------------
# 4. دالة الإغلاق الطارئ (عند تفعيل القفل من الواجهة)
# ---------------------------------------------------------------------------
@app.route('/api/emergency-close', methods=['POST'])
async def emergency_close():
    data = request.json or {}
    session_id = data.get('session_id')
    reason = data.get('reason', 'تم تفعيل الحماية الطارئة')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = get_session(session_id)
    if not session:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    session['is_locked'] = True
    save_session(session_id, session)
    
    try:
        api = MetaApi(API_TOKEN)
        account = await api.metatrader_account_api.get_account(session['account_id'])
        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()
        
        positions = await connection.get_positions()
        for pos in positions:
            pos_id = safe_get_position_id(pos)
            if pos_id:
                try:
                    await connection.close_position(pos_id)
                except:
                    pass
        
        return jsonify({"status": "success", "message": f"تم تفعيل القفل بنجاح: {reason}"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 5. دالة قطع الاتصال ومسح الجلسة (Disconnect)
# ---------------------------------------------------------------------------
@app.route('/api/disconnect', methods=['POST'])
async def disconnect():
    data = request.json or {}
    session_id = data.get('session_id')
    if session_id:
        delete_session(session_id)
    return jsonify({"status": "success", "message": "تم فصل الجلسة بنجاح"}), 200

# ---------------------------------------------------------------------------
# 6. نقطة الصحة (Health Check) لـ Render
# ---------------------------------------------------------------------------
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
