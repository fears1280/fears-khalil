import asyncio
from metaapi_cloud_sdk import MetaApi

# ضع التوكن الخاص بك هنا
API_TOKEN = "ضع_التوكن_الخاص_بك_هنا"

async def register_meta_account():
    # 1. تهيئة الـ API
    api = MetaApi(API_TOKEN)
    account_api = api.metatrader_account_api

    try:
        print("⏳ جاري إرسال البيانات وإنشاء الحساب على سيرفرات MetaAPI...")
        
        # 2. إدخال بيانات الحساب التجريبي (Demo)
        account = await account_api.create_account({
            'name': 'My_Demo_Account',
            'type': 'cloud',          # دائماً cloud لتشغيل السيرفر بالخلفية
            'platform': 'mt5',        # أو 'mt4' حسب حسابك
            'login': '12345678',      # رقم حساب التداول الخاص بك
            'password': 'password',    # كلمة سر الحساب
            'server': 'MetaQuotes-Demo' # اسم السيرفر بالكامل وبدقة
        })

        print(f"✓ تم إنشاء الحساب بنجاح برقم داخلي: {account.id}")
        
        # 3. تفعيل الحساب (Deploy) وهو ضروري جداً ليصبح الـ ID جاهزاً للعمل
        print("⏳ جاري تفعيل وتشغيل السيرفر السحابي للحساب (Deploy)...")
        await account.deploy()
        
        # الانتظار حتى يكتمل الاتصال تماماً
        await account.wait_connected(timeout_in_seconds=60)
        
        print("\n" + "="*40)
        print("🎉 الحساب جاهز ونشط الآن!")
        print(f"👉 Account ID: {account.id}")
        print("="*40)

    except Exception as e:
        print(f"❌ حدث خطأ أثناء التسجيل: {str(e)}")

# تشغيل الكود
if __name__ == "__main__":
    asyncio.run(register_meta_account())
