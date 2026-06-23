from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os

app = Flask(__name__)

API_TOKEN = os.getenv("METAAPI_TOKEN")
api = MetaApi(API_TOKEN)

# دالة مساعدة لإنشاء حلقة عمل آمنة لكل طلب
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

@app.route('/api/connect-user', methods=['POST'])
def connect_user():
    data = request.json
    login = data.get('login')
    password = data.get('password')
    server = data.get('server')
    
    async def register():
        account = await api.metatrader_account_api.create_account({
            'name': f'Guardian_{login}',
            'type': 'cloud',
            'platform': 'mt5',
            'login': str(login),
            'password': password,
            'server': server,
            'magic': 999111
        })
        await account.deploy()
        await account.wait_connected()
        return {"status": "success", "accountId": account.id}

    try:
        result = run_async(register())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run()
