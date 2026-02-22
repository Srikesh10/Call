"""
List all Twilio subaccounts
"""
import os
from dotenv import load_dotenv
from twilio.rest import Client

load_dotenv()

# Get credentials from .env
account_sid = os.environ.get("TWILIO_MASTER_SID")
auth_token = os.environ.get("TWILIO_MASTER_AUTH_TOKEN")

if not account_sid or not auth_token:
    print("❌ Error: TWILIO_MASTER_SID and TWILIO_MASTER_AUTH_TOKEN must be set in .env")
    exit(1)

# Initialize Twilio client
client = Client(account_sid, auth_token)

print("=" * 80)
print("TWILIO SUBACCOUNTS")
print("=" * 80)

# List all accounts (main account + subaccounts)
accounts = client.api.v2010.accounts.list()

subaccounts = []
main_account = None

for account in accounts:
    if account.sid == account_sid:
        main_account = account
    else:
        subaccounts.append(account)

# Display main account
if main_account:
    print(f"\n📌 MAIN ACCOUNT:")
    print(f"   SID:           {main_account.sid}")
    print(f"   Friendly Name: {main_account.friendly_name}")
    print(f"   Status:        {main_account.status}")
    print(f"   Type:          {main_account.type}")

# Display subaccounts
print(f"\n📋 SUBACCOUNTS: {len(subaccounts)}")
print("=" * 80)

if not subaccounts:
    print("\n✅ No subaccounts found!")
else:
    for i, account in enumerate(subaccounts, 1):
        print(f"\n{i}. {account.friendly_name}")
        print(f"   SID:           {account.sid}")
        print(f"   Status:        {account.status}")
        print(f"   Created:       {account.date_created}")
        print(f"   Owner:         {account.owner_account_sid}")

print("\n" + "=" * 80)
print(f"Total Accounts: {len(accounts)} (1 main + {len(subaccounts)} subaccounts)")
print("=" * 80)

# Check limit
print(f"\n💡 Default subaccount limit: 1000")
print(f"   Current usage: {len(subaccounts)}/1000")
if len(subaccounts) >= 1000:
    print("   ⚠️  You've reached the limit! Contact Twilio support for more.")
