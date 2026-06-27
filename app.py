import asyncio
from metaapi_cloud_sdk import MetaApi

# ضع التوكن الحقيقي الخاص بك هنا بدلاً من النص
API_TOKEN = "ضع_التوكن_الخاص_بك_هنا"

async def register_meta_account():
    # 1. تهيئة الـ API
    api = MetaApi(API_TOKEN)
    account_api = api.metatrader_account_api

    try:
        print("⏳ جاري إرسال البيانات وإنشاء الحساب على سيرفرات MetaAPI...")
        
        # 2. إدخال بيانات الحساب التجريبي (Demo) بعد التعديل
        account = await account_api.create_account({
            'name': 'My_Demo_Account',
            'type': 'cloud',          
            'platform': 'mt5',        
            'login': '1200105499',      
            'password': '12345@Feras',    
            'server': 'JustMarkets-Demo3'  # تم تعديل الاسم ليكون دقيقاً وبدون فراغات عشوائية
        })

        print(f"✓ تم إنشاء الحساب بنجاح برقم داخلي: {account.id}")
        
        # 3. تفعيل الحساب (Deploy)
        print("⏳ جاري تفعيل وتشغيل السيرفر السحابي للحساب (Deploy)...")
        await account.deploy()
        
        # الانتظار حتى يكتمل الاتصال تماماً
        await account.wait_connected(timeout_in_seconds=60)
        
        print("\n" + "="*40)
        print("🎉 الحساب جاهز ونشط الآن واشتغل 100%!")
        print(f"👉 Account ID: {account.id}")
        print("="*40)

    except Exception as e:
        print(f"❌ حدث خطأ أثناء التسجيل: {str(e)}")

# تشغيل الكود
if __name__ == "__main__":
    asyncio.run(register_meta_account())
