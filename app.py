import asyncio
from flask import Flask
from metaapi_cloud_sdk import MetaApi

# إنشاء تطبيق وهمي عشان Render ما يعطي خطأ AttributeError
app = Flask(__name__)

# ضع التوكن الحقيقي الخاص بك هنا
API_TOKEN = "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiIwZGUwYjBjOTMzNzkyMTJhZjhjMTRlMzlkODlhOTM0NSIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVzdC1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJtZXRhc3RhdHMtYXBpIiwibWV0aG9kcyI6WyJtZXRhc3RhdHMtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoiY29weWZhY3RvcnktYXBpIiwibWV0aG9kcyI6WyJjb3B5ZmFjdG9yeS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoibXQtbWFuYWdlci1hcGkiLCJtZXRob2RzIjpbIm10LW1hbmFnZXItYXBpOnJlc3Q6ZGVhbGluZzoqOioiLCJtdC1tYW5hZ2VyLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJiaWxsaW5nLWFwaSIsIm1ldGhvZHMiOlsiYmlsbGluZy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiMGRlMGIwYzkzMzc5MjEyYWY4YzE0ZTM5ZDg5YTkzNDUiLCJpYXQiOjE3ODI1Njk5NTUsImV4cCI6MTc5MDM0NTk1NX0.KF2c26NrC9C9aaDudUteh5GCvWLKH5vtIpYfkuz4_olQ9YPPVjEk2HYjPUCykO0f0D-Dco5KGIjEx69XOMS9sc3B61fD7macHZexzL7op1W2igWXyS-Tn8Panx4Z_D8w0jZvH68TTTFtorzZ7MJgpOu-5SQsAHeipEBSJG4yS17P63XJd7qp7yOED2-CU2XhhIs31dPwiC-r9XUZECFyhdRf2Mf5mncrhNUzSWsId8vqF2i-ZFYtcj-REvoiaVtlQ25m3U9rBU1HZP7656IfCBIS0vPSeJcYUonnRikIP_zsraZXCUNHcGcU_wB5tkBdr02FCSjMu82xbcaoMcPNMeQrKKvcMrHsvadLEfWksJ3dmPQsRBFYA9nK3QRCfDkRplkCmul0BFzpnuEKVW4iS4W8d_WE3KE_cKeqRW7SC5qn86yQh6BP9-oFFsoJs9DCm03I07gcfJolN_Jpc7eKUmUbTOrbA2yihR3uqMB_AQbtpmSmoR_AtcfzpPH95c9xXJWEHC7GJcrWQPgHU2veeWSGx-2ClzMS9Hnz1aR8LEp1q0SgRlH1KeivUiWylwcv9NO9-q8U5CO62tsfZuL3KIPA8X2OTTdncTy4b4CPid112MPg6fvGFK06lwPYahWJKox2w0MHkTSqVnMtFJAncTVfZnlh_bmmfNJjJ5CWOrE"

async def register_meta_account():
    api = MetaApi(API_TOKEN)
    account_api = api.metatrader_account_api

    try:
        print("⏳ جاري إرسال البيانات وإنشاء الحساب على سيرفرات MetaAPI...")
        
        account = await account_api.create_account({
            'name': 'My_Demo_Account',
            'type': 'cloud',          
            'platform': 'mt5',        
            'login': '1200105499',      
            'password': '12345@Feras',    
            'server': 'JustMarkets-Demo3'  # تأكد من كتابته كابيتال وبدون فراغات
        })

        print(f"✓ تم إنشاء الحساب بنجاح برقم داخلي: {account.id}")
        
        print("⏳ جاري تفعيل وتشغيل السيرفر السحابي للحساب (Deploy)...")
        await account.deploy()
        
        await account.wait_connected(timeout_in_seconds=60)
        
        print("\n" + "="*40)
        print("🎉 الحساب جاهز ونشط الآن واشتغل 100%!")
        print(f"👉 Account ID: {account.id}")
        print("="*40)

    except Exception as e:
        print(f"❌ حدث خطأ أثناء التسجيل: {str(e)}")

# عند تشغيل السيرفر، سينفذ التسجيل أولاً ثم يطبع النتيجة بالـ Logs
@app.route('/')
def home():
    return "سيرفر التسجيل يعمل بنجاح!"

# تشغيل دالة التسجيل أوتوماتيكياً قبل بدء تطبيق الويب
try:
    asyncio.run(register_meta_account())
except Exception as e:
    print(f"خطأ تشغيل الـ Async: {e}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
