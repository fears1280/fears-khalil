import metaapi_cloud_sdk
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/debug')
def debug():
    # هذا السطر سيكشف لنا الحقيقة في الـ Logs
    return jsonify({
        "version": getattr(metaapi_cloud_sdk, "__version__", "unknown"),
        "has_get_accounts": hasattr(metaapi_cloud_sdk.metaapi.metatrader_account_api.MetatraderAccountApi, 'get_accounts')
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
