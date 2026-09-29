"""Notification texts captured on the owner's device (2026-09-26 to 2026-09-29).

Wording, amounts, masking style, and package names are verbatim. Personal names are
replaced with the fictional owner "Raka Purnama Sentosa" (masked the way myBCA masks,
e.g. "RA*A ***NAMA *E") and a fictional masked third party.
"""

OWNER_ALIAS = "Raka Purnama Sentosa"

# (label, package, title, text, facts status, direction, amount)
REAL_NOTIFICATIONS = [
    ("mybca-spent-misc", "com.bca.mybca.omni.android", "Financial Diary",
     "You spent IDR 332,640.00 at Miscellaneous.", "candidate", "expense", 332640),
    ("mybca-spent-shopping", "com.bca.mybca.omni.android", "Financial Diary",
     "You spent IDR 21,475.00 at Shopping.", "candidate", "expense", 21475),
    ("mybca-received-third-party", "com.bca.mybca.omni.android", "Financial Diary",
     "You received IDR 43,000.00 from ****TA LA**IT M at Account Transfer category.", "candidate", "income", 43000),
    ("jago-moved-out-of-pocket", "com.jago.digitalBanking", "Jago",
     "You've moved Rp1.500.000 out of your My Emergency Fund Pocket. Need help? Contact Tanya Jago at 1500 746.",
     "candidate", "internal_movement", 1500000),
    ("mybca-received-masked-owner", "com.bca.mybca.omni.android", "Financial Diary",
     "You received IDR 1,500,000.00 from RA*A ***NAMA *E at Account Transfer category.", "candidate", "income", 1500000),
    ("mybca-spent-payment", "com.bca.mybca.omni.android", "Financial Diary",
     "You spent IDR 1,385,800.00 at Payment.", "candidate", "expense", 1385800),
    ("mybca-spent-account-transfer", "com.bca.mybca.omni.android", "Financial Diary",
     "You spent IDR 16,700.00 at Account Transfer.", "candidate", "expense", 16700),
    ("jago-pocket-to-pocket", "com.jago.digitalBanking", "Jago",
     "You've moved Rp100.000 from My Emergency Fund Pocket to your Subscriptions Pocket. Click here to see how to add money to Jago. Need help? Contact Tanya Jago at 1500 746.",
     "candidate", "internal_movement", 100000),
    ("jago-debit-card", "com.jago.digitalBanking", "Jago",
     "You've paid Rp103.440 using your debit card. Need help? Contact Tanya Jago at 1500 746.",
     "candidate", "expense", 103440),
    ("mybca-salary", "com.bca.mybca.omni.android", "Financial Diary",
     "You received IDR 7,436,000.00 at Salary category.", "candidate", "income", 7436000),
    ("shopeepay-top-up", "com.shopeepay.id", "Isi Saldo Berhasil",
     "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp7.491.557.",
     "candidate", "income", 7490557),
    ("mybca-spent-top-up-leg", "com.bca.mybca.omni.android", "Financial Diary",
     "You spent IDR 7,490,557.00 at Shopping.", "candidate", "expense", 7490557),
    ("jago-has-sent-owner", "com.jago.digitalBanking", "Jago",
     "Raka Purnama Sentosa has sent Rp7.491.557 to you. Need help? Contact Tanya Jago at 1500 746.",
     "candidate", "income", 7491557),
    ("shopeepay-bifast-owner", "com.shopeepay.id", "Saldo ShopeePay diterima!",
     "RAKA PURNAMA SENTOSA mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu melalui BI-Fast.",
     "candidate", "income", 7491557),
    ("mybca-rdn-earning", "com.bca.mybca.omni.android", "Financial Diary",
     "RDN earning of IDR 225,180.00 at Account Transfer category.", "candidate", "income", 225180),
    ("jago-transferred-to-owner", "com.jago.digitalBanking", "Jago",
     "You've transferred Rp625.000 to RAKA PURNAMA SEN. Need help? Contact Tanya Jago at 1500 746.",
     "candidate", "expense", 625000),
    ("gopay-coins-promotion", "com.gojek.gopay", "Coins GRATIS buat Raka Purnama",
     "Yuk, ambil GRATIS 11,200 GoPay Coins & voucher kamu. Cukup check-in di A+ Rewards! Check-in sekarang~",
     "ignored", None, None),
]
