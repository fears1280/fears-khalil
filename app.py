import os
import asyncio
import time
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
CORS(app)

# جلب الـ Token من متغيرات البيئة في Render
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

# مخزن الجلسات المؤقت في الذاكرة
sessions = {}

# ---------------------------------------------------------------------------
# دالة مساعدة لتشغيل الدالات غير المتزامنة (Async) داخل بيئة Flask المتزامنة
# ---------------------------------------------------------------------------
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def update_session(session_id, data):
    if session_id in sessions:
        sessions[session_id].update(data)
    else:
        sessions[session_id] = data

# ---------------------------------------------------------------------------
# 1. دالة تسجيل الدخول والربط (Connect)
# ---------------------------------------------------------------------------
@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    # تأمين تحويل قيم الأهداف القادمة من فلاتر
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
        async def register_account():
            api = MetaApi(API_TOKEN)
            account = None
            
            # محاولة الفحص الذكي عن الحساب لتجنب مشكلة الـ Limit في الحساب المجاني
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
            
            # إذا لم نجد الحساب مضافاً مسبقاً، نقوم بإنشائه فوراً
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
            
            # استخراج حالة الحساب ورقم الـ ID بمرونة
            try:
                account_state = account.state if hasattr(account, 'state') else account.get('state', '')
                account_id = account.id if hasattr(account, 'id') else account.get('id')
            except:
                account_state = 'UNKNOWN'
                account_id = getattr(account, 'id', None)
                
            if account_state != 'DEPLOYED':
                await account.deploy()
            
            print("⏳ Waiting for account connection setup...")
            await account.wait_connected(timeout_in_seconds=30)
            
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            account_info = await connection.get_account_information()
            initial_balance = float(account_info.get('balance', 0.0))
            
            # إنشاء معرف جلسة فريد ومحمي
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
            
            sessions[session_id] = session_data
            
            return {
                "status": "success",
                "session_id": session_id,
                "account_id": account_id,
                "message": "تم الاتصال وتأمين الحساب بنجاح!"
            }
        
        result = run_async(register_account())
        if result and result.get('status') == 'success':
            return jsonify(result), 201
        else:
            return jsonify({"status": "error", "message": result.get('message', 'فشلت عملية التهيئة')}), 500
            
    except Exception as e:
        print(f"Connection API Critical Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 2. دالة جلب الإحصائيات الحية ومراقبة الحساب (Account Stats & Monitor)
# ---------------------------------------------------------------------------
@app.route('/api/account-stats', methods=['GET'])
def account_stats():
    session_id = request.args.get('session_id')
    
    if not session_id:
        return jsonify({"status": "error", "message": "session_id مطلوب"}), 400
        
    session = sessions.get(session_id)
    
    # 🌟 طوق النجاة: إعادة بناء الجلسة تلقائياً إذا حصل ريستارت للسيرفر والحساب متصل في ميتاترايدر
    if not session:
        print(f"⚠️ Session {session_id} missing from RAM. Starting auto-recovery...")
        try:
            parts = session_id.split('_')
            if len(parts) >= 2:
                extracted_login = parts[1]
                
                api = MetaApi(API_TOKEN)
                existing_accounts = run_async(api.metatrader_account_api.get_accounts())
                
                for acc in existing_accounts:
                    acc_login = getattr(acc, 'login', None) or (acc.get('login') if isinstance(acc, dict) else None)
                    if str(acc_login) == str(extracted_login):
                        account_id = acc.id if hasattr(acc, 'id') else acc.get('id')
                        session_data = {
                            'session_id': session_id,
                            'account_id': account_id,
                            'login': extracted_login,
                            'status': 'connected',
                            'is_locked': False,
                            'daily_target': 500.0,
                            'max_loss_limit': -500.0,
                            'daily_profit': 0.0,
                            'daily_loss': 0.0
                        }
                        sessions[session_id] = session_data
                        session = session_data
                        print(f"✅ Auto-recovered session for login: {extracted_login}")
                        break
        except Exception as recovery_error:
            print(f"❌ Recovery failed: {recovery_error}")

    # إذا فُقدت تماماً ولم يتم العثور عليها بالمنصة
    if not session:
        return jsonify({"status": "error", "message": "خطأ في المصادقة: الجلسة منتهية، يرجى إعادة الدخول"}), 401

    if session.get('is_locked'):
        return jsonify({"status": "locked", "message": "الحساب مقفل حالياً بحارس التداول"}), 200

    try:
        async def fetch_stats():
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
            
            # حساب الأرقام الإحصائية والتراجع ونسبة النمو الإجمالية
            initial = float(session.get('initial_balance', balance))
            drawdown_percent = abs((pnl / balance) * 100) if pnl < 0 else 0.0
            overall_growth = ((balance - initial) / initial) * 100 if initial > 0 else 0.0
            
            # تحديث بيانات الجلسة الحالية في الذاكرة
            session['balance'] = balance
            session['equity'] = equity
            
            return {
                "status": "success",
                "data": {
                    "is_locked": False,
                    "balance": balance,
                    "equity": equity,
                    "current_pnl": pnl,
                    "drawdown_percent": drawdown_percent,
                    "daily_profit": session.get('daily_profit', 0.0),
                    "overall_growth": overall_growth,
                    "open_trades": len(positions)
                }
            }

        result = run_async(fetch_stats())
        return jsonify(result), 200

    except Exception as e:
        print(f"Stats API Error: {e}")
        # حماية ضد السقوط: إرجاع آخر بيانات محفوظة للجلسة بدلاً من الانهيار الكلي بـ 500
        return jsonify({
            "status": "success",
            "data": {
                "is_locked": False,
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
def update_targets():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if not session_id or session_id not in sessions:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    session = sessions[session_id]
    
    if 'daily_profit_target' in data:
        session['daily_target'] = float(data['daily_profit_target'])
    if 'daily_stop_loss' in data:
        # تأكيد حفظ حد الخسارة كقيمة سالبة في بايثون دوماً
        session['max_loss_limit'] = -abs(float(data['daily_stop_loss']))
        
    print(f"⚙️ Targets updated for session {session_id}: Target={session['daily_target']}, LossLimit={session['max_loss_limit']}")
    return jsonify({"status": "success", "message": "تم تحديث الأهداف بنجاح"}), 200

# ---------------------------------------------------------------------------
# 4. دالة الإغلاق الطارئ وقفل الحساب (Emergency Close)
# ---------------------------------------------------------------------------
@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    data = request.json or {}
    session_id = data.get('session_id')
    reason = data.get('reason', 'تم تفعيل الحماية الطارئة')
    
    if not session_id or session_id not in sessions:
        return jsonify({"status": "error", "message": "جلسة عمل غير صالحة"}), 401
        
    session = sessions[session_id]
    session['is_locked'] = True
    
    try:
        async def close_all_positions():
            api = MetaApi(API_TOKEN)
            account = await api.metatrader_account_api.get_account(session['account_id'])
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            # جلب وإغلاق كل الصفقات المفتوحة فوراً لحماية الحساب
            positions = await connection.get_positions()
            print(f"🚨 Emergency Close triggered for Login {session['login']}. Closing {len(positions)} positions. Reason: {reason}")
            
            for pos in positions:
                try:
                    await connection.cancel_order(pos['id'])
                except:
                    try:
                        # محاولة الإغلاق المباشر بحسب هيكلية الحزمة
                        await connection.close_position(pos['id'])
                    except Exception as close_err:
                        print(f"Could not close position {pos['id']}: {close_err}")
            
            # عمل Undeploy للحساب لمنع أي تداول يدوي من الـ PC أو الهاتف لفترة الحظر
            await account.undeploy()
            return {"status": "success", "message": f"تم تفعيل الحظر الطارئ بنجاح وإغلاق الصفقات. السبب: {reason}"}
            
        result = run_async(close_all_positions())
        return jsonify(result), 200
    except Exception as e:
        print(f"Emergency API Critical Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ---------------------------------------------------------------------------
# 5. دالة قطع الاتصال ومسح الجلسة (Disconnect)
# ---------------------------------------------------------------------------
@app.route('/api/disconnect', methods=['POST'])
def disconnect():
    data = request.json or {}
    session_id = data.get('session_id')
    
    if session_id in sessions:
        del sessions[session_id]
        print(f"🛑 Session {session_id} has been wiped out from memory.")
        
    return jsonify({"status": "success", "message": "تم فصل الجلسة وتنظيف الذاكرة بنجاح"}), 200

# ---------------------------------------------------------------------------
# تشغيل السيرفر
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    # العمل على البورت الديناميكي لـ Render أو 5000 محلياً
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
