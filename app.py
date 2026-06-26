import os
import asyncio
from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi

app = Flask(__name__)
API_TOKEN = os.environ.get('METAAPI_TOKEN', '')

@app.route('/', methods=['GET'])
def home():
    return "API is ready", 200

@app.route('/api/connect', methods=['POST'])
def connect():
    data = request.json or {}
    login = str(data.get('login'))
    password = str(data.get('password'))
    server = str(data.get('server'))
    
    # حماية من البيانات الفارغة
    if not login or not password or not server:
        return jsonify({"status": "error", "message": "Missing credentials"}), 400

    async def run_logic():
        api = MetaApi(API_TOKEN)
        # الوصول المباشر للمكتبة
        account_api = api.metatrader_account_api
        
        # استخدام try/except هنا في حال فشل الاتصال
        try:
            # محاولة البحث عن الحساب
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
            return {"account_id": account.id}
            
        except AttributeError:
            return {"error": "المكتبة غير متوافقة (تحتاج تحديث)"}
        except Exception as e:
            return {"error": str(e)}

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(run_logic())
    
    if "error" in result:
        return jsonify({"status": "error", "message": result["error"]}), 500
    return jsonify({"status": "success", "account_id": result["account_id"]}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
