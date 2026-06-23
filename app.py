from flask import Flask, request, jsonify
from flask_cors import CORS
import random
import os
import json
import threading
import time
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# الإعدادات
# ---------------------------------------------------------------------------
SESSION_FILE = "guardian_session.json"

# ---------------------------------------------------------------------------
# دوال مساعدة
# ---------------------------------------------------------------------------
def load_session():
    """تحميل الجلسة من الملف"""
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_session(data):
    """حفظ الجلسة إلى الملف"""
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ---------------------------------------------------------------------------
# محاكي البيانات الحية (يولد أرقاماً متغيرة باستمرار)
# ---------------------------------------------------------------------------
def simulator_loop():
    """حلقة محاكاة تولد بيانات حية متغيرة للتجربة"""
    print("🔄 بدأ محاكي البيانات الحية...")
    
    while True:
        try:
            session = load_session()
            
            # لا نولد بيانات إذا لم يكن هناك حساب متصل
            if not session.get("account_id"):
                time.sleep(2)
                continue
            
            # إذا كان الحساب مقفلاً
            if session.get("is_locked"):
                # التحقق من انتهاء مدة القفل
                lock_until_str = session.get("lock_until")
                if lock_until_str:
                    lock_until = datetime.fromisoformat(lock_until_str)
                    if datetime.now(timezone.utc) >= lock_until:
                        print("🔓 انتهت مدة القفل - إعادة تفعيل الحساب")
                        session["is_locked"] = False
                        session["lock_until"] = None
                        session["latest_stats"] = None
                        save_session(session)
                time.sleep(2)
                continue
            
            # توليد بيانات حية متغيرة
            base_balance = float(session.get("base_balance", 10000.0))
            
            # إضافة تقلب عشوائي للرصيد
            balance_change = random.uniform(-50, 80)
            current_balance = base_balance + balance_change
            
            # الـ equity يتغير بشكل مختلف قليلاً
            equity_change = random.uniform(-30, 60)
            current_equity = current_balance + equity_change
            
            # حساب المؤشرات
            current_pnl = round(current_equity - base_balance, 2)
            drawdown = round(abs((current_balance - current_equity) / current_balance * 100), 2) if current_balance > 0 else 0.0
            daily_profit = round(current_pnl, 2)
            overall_growth = round((current_pnl / base_balance * 100), 2) if base_balance > 0 else 0.0
            open_trades = random.randint(0, 3)
            
            # تحديث الكاش
            stats = {
                "is_locked": False,
                "balance": round(current_balance, 2),
                "equity": round(current_equity, 2),
                "current_pnl": current_pnl,
                "drawdown_percent": drawdown,
                "daily_profit": daily_profit,
                "overall_growth": overall_growth,
                "open_trades": open_trades,
                "remaining_trades": max(0, 4 - open_trades)
            }
            
            session["latest_stats"] = stats
            session["base_balance"] = base_balance + random.uniform(-2, 5)  # تغير بطيء للرصيد الأساسي
            save_session(session)
            
            # طباعة للتشخيص
            print(f"📊 [محاكي] الرصيد: {current_balance:.2f} | الربح: {current_pnl:.2f} | الصفقات: {open_trades}")
            
            # فحص الأهداف
            profit_target = float(session.get("profit_target", 0))
            max_loss = float(session.get("max_loss", 0))
            lockout_hours = float(session.get("lockout_hours", 1))
            
            if profit_target > 0 and current_pnl >= profit_target:
                print(f"🎯 تم تحقيق الهدف: {current_pnl}$")
                session["is_locked"] = True
                session["lock_until"] = (datetime.now(timezone.utc) + timedelta(hours=lockout_hours)).isoformat()
                session["latest_stats"]["is_locked"] = True
                session["latest_stats"]["open_trades"] = 0
                save_session(session)
                
            elif max_loss > 0 and current_pnl <= -abs(max_loss):
                print(f"🛑 تم تجاوز حد الخسارة: {current_pnl}$")
                session["is_locked"] = True
                session["lock_until"] = (datetime.now(timezone.utc) + timedelta(hours=lockout_hours)).isoformat()
                session["latest_stats"]["is_locked"] = True
                session["latest_stats"]["open_trades"] = 0
                save_session(session)
            
        except Exception as e:
            print(f"❌ خطأ في المحاكي: {e}")
        
        time.sleep(2)  # تحديث كل ثانيتين

# ---------------------------------------------------------------------------
# تشغيل المحاكي في خيط منفصل
# ---------------------------------------------------------------------------
simulator_thread = threading.Thread(target=simulator_loop, daemon=True)
simulator_thread.start()

# ---------------------------------------------------------------------------
# 🔌 نقاط API
# ---------------------------------------------------------------------------

@app.route('/api/connect', methods=['POST'])
def connect_account():
    """
    ربط حساب جديد
    يستقبل: { login, password, server, broker_name }
    """
    data = request.json or {}
    login = data.get('login', '')
    password = data.get('password', '')
    server = data.get('server', '')
    broker_name = data.get('broker_name', 'Unknown')
    
    if not login:
        return jsonify({"status": "error", "message": "رقم الحساب مطلوب"}), 400
    
    # إنشاء جلسة جديدة
    session = load_session()
    session["account_id"] = f"{broker_name}_{login}"
    session["login"] = login
    session["server"] = server
    session["broker_name"] = broker_name
    session["is_locked"] = False
    session["lock_until"] = None
    session["base_balance"] = 10000.0 + random.uniform(0, 2000)  # رصيد ابتدائي عشوائي
    session["profit_target"] = 500.0
    session["max_loss"] = 300.0
    session["lockout_hours"] = 2.0
    session["latest_stats"] = None
    save_session(session)
    
    print(f"✅ تم ربط الحساب: {broker_name} - {login} على سيرفر {server}")
    print(f"💰 الرصيد الابتدائي: {session['base_balance']:.2f}$")
    
    return jsonify({
        "status": "success",
        "message": "تم ربط الحساب بنجاح",
        "account_id": session["account_id"],
        "balance": session["base_balance"]
    }), 200


@app.route('/api/account-stats', methods=['GET'])
def get_account_stats():
    """
    جلب إحصائيات الحساب الحية
    يرجع أحدث بيانات من الكاش
    """
    session = load_session()
    
    # إذا لم يكن هناك حساب متصل
    if not session.get("account_id"):
        return jsonify({
            "is_locked": False,
            "balance": 0.0,
            "equity": 0.0,
            "current_pnl": 0.0,
            "drawdown_percent": 0.0,
            "daily_profit": 0.0,
            "overall_growth": 0.0,
            "open_trades": 0,
            "remaining_trades": 4,
            "message": "لا يوجد حساب متصل"
        }), 200
    
    # إذا كان الحساب مقفلاً
    if session.get("is_locked"):
        lock_until_str = session.get("lock_until", "")
        remaining_hours = 0
        if lock_until_str:
            lock_until = datetime.fromisoformat(lock_until_str)
            remaining = lock_until - datetime.now(timezone.utc)
            remaining_hours = max(0, round(remaining.total_seconds() / 3600, 1))
        
        return jsonify({
            "is_locked": True,
            "balance": 0.0,
            "equity": 0.0,
            "current_pnl": 0.0,
            "drawdown_percent": 0.0,
            "daily_profit": 0.0,
            "overall_growth": 0.0,
            "open_trades": 0,
            "remaining_trades": 0,
            "lock_remaining_hours": remaining_hours,
            "message": f"الحساب مقفل - متبقي {remaining_hours} ساعة"
        }), 200
    
    # إرجاع أحدث بيانات من الكاش
    stats = session.get("latest_stats")
    if stats:
        return jsonify(stats), 200
    
    # إذا لم تتوفر بيانات بعد
    return jsonify({
        "is_locked": False,
        "balance": session.get("base_balance", 0.0),
        "equity": session.get("base_balance", 0.0),
        "current_pnl": 0.0,
        "drawdown_percent": 0.0,
        "daily_profit": 0.0,
        "overall_growth": 0.0,
        "open_trades": 0,
        "remaining_trades": 4,
        "message": "جاري تحميل البيانات..."
    }), 200


@app.route('/api/update-targets', methods=['POST'])
def update_targets():
    """
    تحديث الأهداف من التطبيق
    يستقبل: { daily_profit_target, daily_stop_loss, lockout_hours }
    """
    data = request.json or {}
    session = load_session()
    
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب متصل"}), 400
    
    updated = []
    
    if 'daily_profit_target' in data:
        value = float(data['daily_profit_target'])
        session['profit_target'] = value
        updated.append(f"هدف الربح: {value}$")
        print(f"🎯 تم تحديث هدف الربح: {value}$")
    
    if 'daily_stop_loss' in data:
        value = float(data['daily_stop_loss'])
        session['max_loss'] = value
        updated.append(f"حد الخسارة: {value}$")
        print(f"⛔ تم تحديث حد الخسارة: {value}$")
    
    if 'lockout_hours' in data:
        value = float(data['lockout_hours'])
        session['lockout_hours'] = value
        updated.append(f"مدة القفل: {value} ساعة")
        print(f"⏰ تم تحديث مدة القفل: {value} ساعة")
    
    if 'early_warning' in data:
        session['early_warning'] = bool(data['early_warning'])
        updated.append(f"التنبيه المبكر: {data['early_warning']}")
    
    if not updated:
        return jsonify({"status": "error", "message": "لم يتم إرسال أي تحديثات"}), 400
    
    save_session(session)
    
    return jsonify({
        "status": "success",
        "message": f"تم تحديث: {', '.join(updated)}",
        "current_targets": {
            "profit_target": session.get('profit_target', 0),
            "max_loss": session.get('max_loss', 0),
            "lockout_hours": session.get('lockout_hours', 1)
        }
    }), 200


@app.route('/api/emergency-close', methods=['POST'])
def emergency_close():
    """
    إغلاق طارئ من التطبيق
    يستقبل: { reason, lockout_hours }
    """
    data = request.json or {}
    session = load_session()
    
    if not session.get("account_id"):
        return jsonify({"status": "error", "message": "لا يوجد حساب متصل"}), 400
    
    reason = data.get('reason', 'أمر يدوي')
    lockout_hours = float(data.get('lockout_hours', 2))
    
    # تفعيل القفل
    session["is_locked"] = True
    session["lock_until"] = (datetime.now(timezone.utc) + timedelta(hours=lockout_hours)).isoformat()
    
    # تحديث الكاش
    if session.get("latest_stats"):
        session["latest_stats"]["is_locked"] = True
        session["latest_stats"]["open_trades"] = 0
        session["latest_stats"]["current_pnl"] = 0
    else:
        session["latest_stats"] = {
            "is_locked": True,
            "balance": 0.0,
            "equity": 0.0,
            "current_pnl": 0.0,
            "drawdown_percent": 0.0,
            "daily_profit": 0.0,
            "overall_growth": 0.0,
            "open_trades": 0,
            "remaining_trades": 0
        }
    
    save_session(session)
    
    print(f"🚨 إغلاق طارئ: {reason} | مدة القفل: {lockout_hours} ساعة")
    
    return jsonify({
        "status": "success",
        "message": f"تم الإغلاق الطارئ: {reason}",
        "lock_until": session["lock_until"]
    }), 200


@app.route('/api/disconnect', methods=['POST'])
def disconnect_account():
    """
    فصل الحساب وحذف الجلسة
    """
    if os.path.exists(SESSION_FILE):
        os.remove(SESSION_FILE)
        print("🔌 تم فصل الحساب وحذف الجلسة")
    
    return jsonify({
        "status": "success",
        "message": "تم فصل الحساب وتصفير الجلسة بنجاح"
    }), 200


@app.route('/api/status', methods=['GET'])
def server_status():
    """
    نقطة فحص حالة الخادم (للتشخيص)
    """
    session = load_session()
    
    return jsonify({
        "server": "running",
        "version": "1.0.0",
        "simulator": "active",
        "account_connected": bool(session.get("account_id")),
        "is_locked": session.get("is_locked", False),
        "session_summary": {
            "account_id": session.get("account_id"),
            "broker_name": session.get("broker_name"),
            "login": session.get("login"),
            "profit_target": session.get("profit_target"),
            "max_loss": session.get("max_loss"),
            "lockout_hours": session.get("lockout_hours"),
            "has_latest_stats": bool(session.get("latest_stats"))
        }
    }), 200


@app.route('/', methods=['GET'])
def home():
    """الصفحة الرئيسية"""
    return jsonify({
        "name": "Trading Guardian Server",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "POST /api/connect - ربط حساب",
            "GET /api/account-stats - إحصائيات الحساب",
            "POST /api/update-targets - تحديث الأهداف",
            "POST /api/emergency-close - إغلاق طارئ",
            "POST /api/disconnect - فصل الحساب",
            "GET /api/status - حالة الخادم"
        ]
    })

# ---------------------------------------------------------------------------
# تشغيل الخادم
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 60)
    print("🛡️  TRADING GUARDIAN SERVER")
    print("=" * 60)
    print("📡 الخادم يعمل على: http://0.0.0.0:5000")
    print("🔄 المحاكي نشط - يولد بيانات حية كل ثانيتين")
    print("📊 نقاط API جاهزة للاتصال")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=False)
