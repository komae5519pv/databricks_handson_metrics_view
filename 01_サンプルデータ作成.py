# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">01. サンプルデータ作成</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;">⏱ 10分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 1 of 6</span>
# MAGIC   </div>
# MAGIC   <img src="https://cdn.simpleicons.org/databricks/FF3621" width="36" height="36"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックのゴール</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ドラッグストアチェーンのサンプルデータを生成し、<strong>テーブルコメント・カラムコメント</strong>を付与する。<br/>
# MAGIC         以降のハンズオンで使用する 7 テーブルを作成します。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:20px 0 16px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">📋 作成するテーブル</span>
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;">テーブル名</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;">説明</th>
# MAGIC       <th style="padding:10px 16px;text-align:right;">件数目安</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">stores</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">店舗マスタ</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">50</td></tr>
# MAGIC     <tr style="background:#F8F9FA;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">categories</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">カテゴリマスタ</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">7</td></tr>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">sales_daily</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">日別売上</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">~91,000</td></tr>
# MAGIC     <tr style="background:#F8F9FA;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">sales_by_category</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">カテゴリ別月次売上</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">~8,400</td></tr>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">prescription_monthly</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">調剤月次実績</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">~720</td></tr>
# MAGIC     <tr style="background:#F8F9FA;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">members</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">会員マスタ</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">100,000</td></tr>
# MAGIC     <tr style="background:#fff;"><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;">member_visits</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">会員来店月次サマリ</td><td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;text-align:right;">~50,000</td></tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, rand, floor, ceil, to_date, concat, lpad, expr,
    when, round as spark_round, monotonically_increasing_id,
    date_add, add_months, abs as spark_abs
)
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 店舗マスタ（stores）

# COMMAND ----------

regions_data = [
    ("北海道", "札幌市", "北海道", 43.06, 141.35),
    ("北海道", "旭川市", "北海道", 43.77, 142.37),
    ("宮城県", "仙台市", "東北", 38.27, 140.87),
    ("福島県", "郡山市", "東北", 37.40, 140.38),
    ("茨城県", "水戸市", "関東", 36.34, 140.45),
    ("埼玉県", "さいたま市", "関東", 35.86, 139.65),
    ("埼玉県", "川越市", "関東", 35.93, 139.49),
    ("埼玉県", "越谷市", "関東", 35.89, 139.79),
    ("千葉県", "千葉市", "関東", 35.61, 140.11),
    ("千葉県", "船橋市", "関東", 35.69, 139.98),
    ("千葉県", "柏市", "関東", 35.87, 139.97),
    ("東京都", "八王子市", "関東", 35.67, 139.32),
    ("東京都", "町田市", "関東", 35.55, 139.45),
    ("東京都", "立川市", "関東", 35.71, 139.41),
    ("神奈川県", "横浜市", "関東", 35.44, 139.64),
    ("神奈川県", "藤沢市", "関東", 35.34, 139.49),
    ("神奈川県", "相模原市", "関東", 35.57, 139.37),
    ("新潟県", "新潟市", "中部", 37.90, 139.02),
    ("長野県", "長野市", "中部", 36.65, 138.19),
    ("静岡県", "静岡市", "中部", 34.98, 138.38),
    ("静岡県", "浜松市", "中部", 34.71, 137.73),
    ("愛知県", "名古屋市", "東海", 35.18, 136.91),
    ("愛知県", "豊田市", "東海", 35.08, 137.16),
    ("愛知県", "岡崎市", "東海", 34.95, 137.17),
    ("愛知県", "春日井市", "東海", 35.25, 136.97),
    ("愛知県", "豊橋市", "東海", 34.77, 137.39),
    ("岐阜県", "岐阜市", "東海", 35.42, 136.76),
    ("三重県", "四日市市", "東海", 34.97, 136.62),
    ("三重県", "津市", "東海", 34.73, 136.51),
    ("京都府", "京都市", "関西", 35.01, 135.77),
    ("大阪府", "大阪市", "関西", 34.69, 135.50),
    ("大阪府", "堺市", "関西", 34.57, 135.48),
    ("大阪府", "東大阪市", "関西", 34.68, 135.60),
    ("兵庫県", "神戸市", "関西", 34.69, 135.20),
    ("兵庫県", "姫路市", "関西", 34.83, 134.69),
    ("兵庫県", "西宮市", "関西", 34.74, 135.34),
    ("奈良県", "奈良市", "関西", 34.69, 135.80),
    ("岡山県", "岡山市", "中国", 34.66, 133.92),
    ("広島県", "広島市", "中国", 34.40, 132.46),
    ("広島県", "福山市", "中国", 34.49, 133.36),
    ("香川県", "高松市", "四国", 34.34, 134.04),
    ("愛媛県", "松山市", "四国", 33.84, 132.77),
    ("福岡県", "福岡市", "九州", 33.59, 130.40),
    ("福岡県", "北九州市", "九州", 33.88, 130.88),
    ("福岡県", "久留米市", "九州", 33.32, 130.51),
    ("熊本県", "熊本市", "九州", 32.79, 130.74),
    ("大分県", "大分市", "九州", 33.24, 131.61),
    ("鹿児島県", "鹿児島市", "九州", 31.60, 130.56),
    ("沖縄県", "那覇市", "九州", 26.21, 127.68),
    ("沖縄県", "浦添市", "九州", 26.25, 127.72),
]

store_types = ["駅前型", "郊外型", "ロードサイド型"]
stores_data = []

for i, (pref, city, region, lat, lon) in enumerate(regions_data):
    store_id = f"S{str(i+1).zfill(3)}"
    store_name = f"店舗_{city}"
    size_sqm = random.randint(3, 12) * 100  # 300-1200 sqm
    # 最後の5店舗（S046-S050）は2025年新規オープン（前年比デモ用）
    if i >= 45:
        open_year = 2025
        open_date = f"2025-{random.randint(1,6):02d}-01"
    else:
        open_year = random.randint(2005, 2020)
        open_date = f"{open_year}-{random.randint(1,12):02d}-01"
    store_type = random.choice(store_types)
    has_pharmacy = random.random() < 0.6  # 60% に調剤併設
    parking = 0 if store_type == "駅前型" else random.randint(20, 80)

    stores_data.append((
        store_id, store_name, pref, city, region,
        round(lat + random.uniform(-0.03, 0.03), 4),
        round(lon + random.uniform(-0.03, 0.03), 4),
        size_sqm, open_date, store_type, has_pharmacy, parking
    ))

stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("prefecture", StringType(), False),
    StructField("city", StringType(), False),
    StructField("region", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("size_sqm", IntegerType(), False),
    StructField("open_date", StringType(), False),
    StructField("store_type", StringType(), False),
    StructField("has_pharmacy", BooleanType(), False),
    StructField("parking_capacity", IntegerType(), False),
])

df_stores = spark.createDataFrame(stores_data, stores_schema)
df_stores = df_stores.withColumn("open_date", to_date(col("open_date")))
df_stores.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("stores")
print(f"✅ stores: {df_stores.count()} 件")

# COMMAND ----------

display(spark.table("stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. カテゴリマスタ（categories）

# COMMAND ----------

categories_data = [
    ("CAT01", "医薬品（OTC）", 0.15, 0.40, "風邪薬、胃腸薬、鎮痛剤、ビタミン剤等"),
    ("CAT02", "調剤",          0.12, 0.35, "処方箋に基づく医薬品の調剤"),
    ("CAT03", "化粧品",        0.18, 0.30, "スキンケア、メイク、ヘアケア等"),
    ("CAT04", "日用品",        0.20, 0.20, "洗剤、ティッシュ、トイレタリー等"),
    ("CAT05", "食品・飲料",    0.25, 0.15, "菓子、飲料、カップ麺、冷凍食品等"),
    ("CAT06", "健康食品・サプリ", 0.05, 0.45, "サプリメント、プロテイン、青汁等"),
    ("CAT07", "ベビー・介護用品", 0.05, 0.25, "おむつ、ミルク、介護食、介護用品等"),
]

categories_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("sales_ratio", DoubleType(), False),
    StructField("gross_margin", DoubleType(), False),
    StructField("description", StringType(), False),
])

df_categories = spark.createDataFrame(categories_data, categories_schema)
df_categories.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("categories")
print(f"✅ categories: {df_categories.count()} 件")

# COMMAND ----------

display(spark.table("categories"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 日別売上（sales_daily）

# COMMAND ----------

df_stores_pd = df_stores.toPandas()

# 5年間の日付リスト（2021-2025）→ 予測がリアルになる
start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 12, 31)
dates = []
d = start_date
while d <= end_date:
    dates.append(d.strftime("%Y-%m-%d"))
    d += timedelta(days=1)

sales_data = []
for _, store in df_stores_pd.iterrows():
    store_id = store['store_id']
    size = store['size_sqm']
    is_urban = store['store_type'] == "駅前型"
    has_rx = store['has_pharmacy']

    # 基準日販（坪数ベース）
    tsubo = size / 3.3
    base_daily = int(tsubo * random.randint(8000, 18000))
    if has_rx:
        base_daily = int(base_daily * 1.15)  # 調剤併設は+15%

    # 業界全体: 年3-5%の緩やかな成長（ドラッグストア業界の実態に近い）
    annual_growth = random.uniform(0.03, 0.05)
    # 一部の店舗は低迷（15%の確率）
    is_struggling = random.random() < 0.15
    if is_struggling:
        annual_growth = random.uniform(-0.03, 0.01)

    # 新店は開店日以降のみデータ生成
    store_open_date = store['open_date'].strftime("%Y-%m-%d") if hasattr(store['open_date'], 'strftime') else str(store['open_date'])

    for day_idx, date_str in enumerate(dates):
        if date_str < store_open_date:
            continue
        month_num = int(date_str.split("-")[1])
        dow = datetime.strptime(date_str, "%Y-%m-%d").weekday()  # 0=Mon

        # 年次成長トレンド（5年間で安定的に成長）
        years_elapsed = day_idx / 365.0
        trend = (1 + annual_growth) ** years_elapsed

        # 季節変動（ドラッグストアの明確な季節パターン）
        seasonal_map = {
            1: 1.12,   # 冬: インフルエンザ、風邪薬
            2: 1.10,   # 冬: 花粉症の始まり
            3: 1.15,   # 春: 花粉症ピーク、新生活
            4: 1.05,   # 春: 新生活
            5: 0.98,   # 端境期
            6: 0.95,   # 梅雨: 来店減
            7: 1.03,   # 夏: 日焼け止め、制汗剤
            8: 1.05,   # 夏: 帰省需要
            9: 0.93,   # 端境期
            10: 0.95,  # 秋
            11: 0.97,  # 秋
            12: 1.08,  # 年末: まとめ買い
        }
        seasonal = seasonal_map[month_num] * random.uniform(0.97, 1.03)

        # 曜日変動（土日は来客増、月曜は少ない）
        if dow >= 5:
            dow_factor = random.uniform(1.20, 1.35)
        elif dow == 0:
            dow_factor = random.uniform(0.85, 0.95)
        else:
            dow_factor = random.uniform(0.95, 1.05)

        noise = random.uniform(0.93, 1.07)
        sales_amount = int(base_daily * trend * seasonal * dow_factor * noise)
        customer_count = int(sales_amount / random.randint(1200, 2800))
        receipt_count = int(customer_count * random.uniform(0.85, 1.0))

        sales_data.append((store_id, date_str, sales_amount, max(1, customer_count), max(1, receipt_count)))

sales_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("sales_date", StringType(), False),
    StructField("sales_amount", IntegerType(), False),
    StructField("customer_count", IntegerType(), False),
    StructField("receipt_count", IntegerType(), False),
])

df_sales = spark.createDataFrame(sales_data, sales_schema)
df_sales = df_sales.withColumn("sales_date", to_date(col("sales_date")))
df_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_daily")
print(f"✅ sales_daily: {df_sales.count()} 件")

# COMMAND ----------

display(spark.table("sales_daily").orderBy("store_id", "sales_date").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. カテゴリ別月次売上（sales_by_category）

# COMMAND ----------

# 月リスト（5年間: 2021-2025）
months = []
m = datetime(2021, 1, 1)
for i in range(60):  # 5年 = 60ヶ月
    months.append((m + relativedelta(months=i)).strftime("%Y-%m-01"))

cat_ids = [c[0] for c in categories_data]
cat_ratios = [c[2] for c in categories_data]
# カテゴリインデックス: CAT05=食品・飲料 は index 4
FOOD_IDX = 4

sales_by_cat_data = []
for _, store in df_stores_pd.iterrows():
    store_id = store['store_id']
    has_rx = store['has_pharmacy']

    # 店舗ごとのカテゴリ偏り
    store_hash = hash(store_id) % 100
    base_adjusted = []
    for i, r in enumerate(cat_ratios):
        adj = r * (1 + ((store_hash + i * 13) % 40 - 20) / 100)
        if cat_ids[i] == "CAT02" and not has_rx:
            adj = 0  # 調剤非併設は調剤売上なし
        base_adjusted.append(adj)

    # 新店は開店月以降のみ
    store_open_str = store['open_date'].strftime("%Y-%m-01") if hasattr(store['open_date'], 'strftime') else str(store['open_date'])[:7] + "-01"

    for month_idx, month_str in enumerate(months):
        if month_str < store_open_str:
            continue
        month_num = int(month_str.split("-")[1])
        year_num = int(month_str.split("-")[0])

        # 食品構成比の年次上昇トレンド（フード&ドラッグ戦略）
        # 2021年を基準に、食品は年+2%pt ずつ構成比が上昇
        years_from_base = (year_num - 2021) + (month_num - 1) / 12.0
        food_boost = 1 + (years_from_base * 0.08)  # 5年で約40%増

        adjusted = list(base_adjusted)
        adjusted[FOOD_IDX] = adjusted[FOOD_IDX] * food_boost
        total_w = sum(adjusted)
        normalized = [w / total_w for w in adjusted]

        # 月次売上の概算（年次成長トレンド付き）
        growth_factor = (1 + 0.04) ** years_from_base  # 年4%成長
        base_monthly = store['size_sqm'] / 3.3 * random.randint(8000, 18000) * 30 * growth_factor

        # 季節変動
        seasonal_map = {1: 1.12, 2: 1.10, 3: 1.15, 4: 1.05, 5: 0.98, 6: 0.95,
                        7: 1.03, 8: 1.05, 9: 0.93, 10: 0.95, 11: 0.97, 12: 1.08}
        seasonal = seasonal_map[month_num]

        for cat_id, weight in zip(cat_ids, normalized):
            cat_sales = int(base_monthly * weight * seasonal * random.uniform(0.90, 1.10))
            if cat_sales > 0:
                sales_by_cat_data.append((store_id, cat_id, month_str, cat_sales))

sbc_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("category_id", StringType(), False),
    StructField("month", StringType(), False),
    StructField("sales_amount", IntegerType(), False),
])

df_sbc = spark.createDataFrame(sales_by_cat_data, sbc_schema)
df_sbc = df_sbc.withColumn("month", to_date(col("month")))
df_sbc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_by_category")
print(f"✅ sales_by_category: {df_sbc.count()} 件")

# COMMAND ----------

display(spark.table("sales_by_category").orderBy("store_id", "category_id", "month").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 調剤月次実績（prescription_monthly）

# COMMAND ----------

rx_stores = [s for s in stores_data if s[10] == True]  # has_pharmacy=True
rx_data = []

for store in rx_stores:
    store_id = store[0]
    store_open_str = store[8][:7] + "-01"  # open_date から年月を取得
    for month_str in months:
        if month_str < store_open_str:
            continue
        month_num = int(month_str.split("-")[1])
        year_num = int(month_str.split("-")[0])

        # 処方箋枚数（月1000-3000枚、年次で微増トレンド）
        years_from_base = (year_num - 2021) + (month_num - 1) / 12.0
        rx_growth = (1 + 0.03) ** years_from_base  # 年3%増（高齢化）
        base_scripts = int(random.randint(1000, 3000) * rx_growth)

        seasonal_map = {1: 1.15, 2: 1.12, 3: 1.10, 4: 1.02, 5: 0.98, 6: 0.95,
                        7: 0.97, 8: 0.98, 9: 0.96, 10: 1.00, 11: 1.03, 12: 1.08}
        seasonal = seasonal_map[month_num]
        scripts = int(base_scripts * seasonal * random.uniform(0.90, 1.10))

        # 処方箋単価（技術料込み8000-12000円）
        unit_price = random.randint(8000, 12000)
        rx_sales = scripts * unit_price

        # 技術料（調剤基本料+薬学管理料 etc.）
        technical_fee = int(scripts * random.randint(1500, 2500))

        rx_data.append((store_id, month_str, scripts, rx_sales, technical_fee))

rx_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("month", StringType(), False),
    StructField("prescription_count", IntegerType(), False),
    StructField("rx_sales_amount", IntegerType(), False),
    StructField("technical_fee", IntegerType(), False),
])

df_rx = spark.createDataFrame(rx_data, rx_schema)
df_rx = df_rx.withColumn("month", to_date(col("month")))
df_rx.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("prescription_monthly")
print(f"✅ prescription_monthly: {df_rx.count()} 件")

# COMMAND ----------

display(spark.table("prescription_monthly").orderBy("store_id", "month").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 会員マスタ（members）

# COMMAND ----------

age_groups = ["10代", "20代", "30代", "40代", "50代", "60代", "70代以上"]
age_weights = [0.03, 0.12, 0.18, 0.22, 0.20, 0.15, 0.10]
genders = ["男性", "女性"]
ranks = ["レギュラー", "シルバー", "ゴールド", "プラチナ"]
rank_weights = [0.50, 0.25, 0.15, 0.10]

member_data = []
all_store_ids = [s[0] for s in stores_data]

for i in range(100000):
    member_id = f"M{str(i+1).zfill(6)}"
    age_group = random.choices(age_groups, weights=age_weights, k=1)[0]
    gender = random.choice(genders)
    rank = random.choices(ranks, weights=rank_weights, k=1)[0]
    home_store = random.choice(all_store_ids)
    join_date = f"{random.randint(2015, 2025)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

    member_data.append((member_id, age_group, gender, rank, home_store, join_date))

member_schema = StructType([
    StructField("member_id", StringType(), False),
    StructField("age_group", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("member_rank", StringType(), False),
    StructField("home_store_id", StringType(), False),
    StructField("join_date", StringType(), False),
])

df_members = spark.createDataFrame(member_data, member_schema)
df_members = df_members.withColumn("join_date", to_date(col("join_date")))
df_members.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("members")
print(f"✅ members: {df_members.count()} 件")

# COMMAND ----------

display(spark.table("members").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 会員来店月次サマリ（member_visits）

# COMMAND ----------

# サンプリング：上位5万人分
sampled_members = random.sample(member_data, 50000)
visit_data = []

for member in sampled_members:
    member_id = member[0]
    rank = member[3]
    home_store = member[4]

    # ランクで来店頻度が変わる
    base_visits = {"レギュラー": 2, "シルバー": 4, "ゴールド": 6, "プラチナ": 10}
    base = base_visits[rank]

    # 直近6ヶ月分
    for i in range(6):
        month_dt = datetime(2025, 7, 1) + relativedelta(months=i)
        month_str = month_dt.strftime("%Y-%m-01")
        visits = max(1, int(base * random.uniform(0.5, 1.5)))
        purchase_amount = visits * random.randint(800, 3500)

        visit_data.append((member_id, home_store, month_str, visits, purchase_amount))

visit_schema = StructType([
    StructField("member_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("month", StringType(), False),
    StructField("visit_count", IntegerType(), False),
    StructField("purchase_amount", IntegerType(), False),
])

df_visits = spark.createDataFrame(visit_data, visit_schema)
df_visits = df_visits.withColumn("month", to_date(col("month")))
df_visits.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("member_visits")
print(f"✅ member_visits: {df_visits.count()} 件")

# COMMAND ----------

display(spark.table("member_visits").orderBy("member_id", "month").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. データ確認

# COMMAND ----------

tables = ["stores", "categories", "sales_daily", "sales_by_category", "prescription_monthly", "members", "member_visits"]

print("=" * 50)
print("作成したテーブル一覧")
print("=" * 50)
for table in tables:
    count = spark.table(table).count()
    print(f"{table:30} : {count:>10,} 件")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. メタデータ付与

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         テーブルコメント・カラムコメントは <strong>Genie の回答精度</strong>と <strong>Metric View 定義時の Genie Code の精度</strong>を大きく向上させます。<br/>
# MAGIC         必ず付与しましょう。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルコメント
# MAGIC COMMENT ON TABLE stores IS 'ドラッグストア店舗マスタ。全店舗の基本情報（所在地、売場面積、店舗タイプ、調剤併設有無、駐車場台数）を管理';
# MAGIC COMMENT ON TABLE categories IS 'カテゴリマスタ。医薬品/調剤/化粧品/日用品/食品/サプリ/介護の7カテゴリを定義。売上構成比と粗利率を保持';
# MAGIC COMMENT ON TABLE sales_daily IS '日別売上。店舗ごとの日次売上実績（売上金額、客数、レシート枚数）。2021年1月〜2025年12月の5年分';
# MAGIC COMMENT ON TABLE sales_by_category IS 'カテゴリ別月次売上。店舗×カテゴリ×月ごとの売上金額';
# MAGIC COMMENT ON TABLE prescription_monthly IS '調剤月次実績。調剤併設店の処方箋枚数、調剤売上、技術料。2021年1月〜2025年12月';
# MAGIC COMMENT ON TABLE members IS '会員マスタ。ポイントカード会員の年代、性別、会員ランク、所属店舗';
# MAGIC COMMENT ON TABLE member_visits IS '会員来店月次サマリ。会員ごとの月別来店回数と購入金額';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- stores カラムコメント
# MAGIC ALTER TABLE stores ALTER COLUMN store_id COMMENT '店舗ID（主キー）。形式: S001〜S050';
# MAGIC ALTER TABLE stores ALTER COLUMN store_name COMMENT '店舗名。形式: 店舗_{市区町村名}';
# MAGIC ALTER TABLE stores ALTER COLUMN prefecture COMMENT '都道府県';
# MAGIC ALTER TABLE stores ALTER COLUMN city COMMENT '市区町村';
# MAGIC ALTER TABLE stores ALTER COLUMN region COMMENT '地域（北海道/東北/関東/中部/東海/関西/中国/四国/九州）';
# MAGIC ALTER TABLE stores ALTER COLUMN latitude COMMENT '緯度';
# MAGIC ALTER TABLE stores ALTER COLUMN longitude COMMENT '経度';
# MAGIC ALTER TABLE stores ALTER COLUMN size_sqm COMMENT '売場面積（平方メートル）。300〜1200sqm';
# MAGIC ALTER TABLE stores ALTER COLUMN open_date COMMENT '開店日';
# MAGIC ALTER TABLE stores ALTER COLUMN store_type COMMENT '店舗タイプ（駅前型/郊外型/ロードサイド型）';
# MAGIC ALTER TABLE stores ALTER COLUMN has_pharmacy COMMENT '調剤併設有無。true=調剤薬局を併設';
# MAGIC ALTER TABLE stores ALTER COLUMN parking_capacity COMMENT '駐車場台数。駅前型は0台';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sales_daily カラムコメント
# MAGIC ALTER TABLE sales_daily ALTER COLUMN store_id COMMENT '店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE sales_daily ALTER COLUMN sales_date COMMENT '売上日';
# MAGIC ALTER TABLE sales_daily ALTER COLUMN sales_amount COMMENT '売上金額（円）';
# MAGIC ALTER TABLE sales_daily ALTER COLUMN customer_count COMMENT '客数（ユニーク来店者数）';
# MAGIC ALTER TABLE sales_daily ALTER COLUMN receipt_count COMMENT 'レシート枚数';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- categories カラムコメント
# MAGIC ALTER TABLE categories ALTER COLUMN category_id COMMENT 'カテゴリID（主キー）。CAT01〜CAT07';
# MAGIC ALTER TABLE categories ALTER COLUMN category_name COMMENT 'カテゴリ名';
# MAGIC ALTER TABLE categories ALTER COLUMN sales_ratio COMMENT '全社平均の売上構成比（0〜1）';
# MAGIC ALTER TABLE categories ALTER COLUMN gross_margin COMMENT '粗利率（0〜1）';
# MAGIC ALTER TABLE categories ALTER COLUMN description COMMENT 'カテゴリの説明・含まれる商品例';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sales_by_category カラムコメント
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN store_id COMMENT '店舗ID（外部キー）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN category_id COMMENT 'カテゴリID（外部キー → categories.category_id）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN month COMMENT '対象年月（月初日）';
# MAGIC ALTER TABLE sales_by_category ALTER COLUMN sales_amount COMMENT '売上金額（円）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- prescription_monthly カラムコメント
# MAGIC ALTER TABLE prescription_monthly ALTER COLUMN store_id COMMENT '店舗ID（外部キー）。調剤併設店のみ';
# MAGIC ALTER TABLE prescription_monthly ALTER COLUMN month COMMENT '対象年月（月初日）';
# MAGIC ALTER TABLE prescription_monthly ALTER COLUMN prescription_count COMMENT '処方箋枚数';
# MAGIC ALTER TABLE prescription_monthly ALTER COLUMN rx_sales_amount COMMENT '調剤売上金額（円）。技術料+薬剤料の合計';
# MAGIC ALTER TABLE prescription_monthly ALTER COLUMN technical_fee COMMENT '技術料（円）。調剤基本料+薬学管理料等';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- members カラムコメント
# MAGIC ALTER TABLE members ALTER COLUMN member_id COMMENT '会員ID（主キー）。形式: M000001〜M100000';
# MAGIC ALTER TABLE members ALTER COLUMN age_group COMMENT '年代（10代/20代/30代/40代/50代/60代/70代以上）';
# MAGIC ALTER TABLE members ALTER COLUMN gender COMMENT '性別（男性/女性）';
# MAGIC ALTER TABLE members ALTER COLUMN member_rank COMMENT '会員ランク（レギュラー/シルバー/ゴールド/プラチナ）';
# MAGIC ALTER TABLE members ALTER COLUMN home_store_id COMMENT '所属店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE members ALTER COLUMN join_date COMMENT '入会日';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- member_visits カラムコメント
# MAGIC ALTER TABLE member_visits ALTER COLUMN member_id COMMENT '会員ID（外部キー → members.member_id）';
# MAGIC ALTER TABLE member_visits ALTER COLUMN store_id COMMENT '来店店舗ID（外部キー → stores.store_id）';
# MAGIC ALTER TABLE member_visits ALTER COLUMN month COMMENT '対象年月（月初日）';
# MAGIC ALTER TABLE member_visits ALTER COLUMN visit_count COMMENT '月間来店回数';
# MAGIC ALTER TABLE member_visits ALTER COLUMN purchase_amount COMMENT '月間購入金額（円）';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. PK / FK 制約

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">PK / FK 制約の効果</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Unity Catalog の PK/FK 制約は<strong>情報提供目的</strong>（informational）です。実行時の強制はされませんが、<br/>
# MAGIC         <strong>Metric View の JOIN 定義</strong>や <strong>Genie のテーブル間関係の推論</strong>に活用されます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOT NULL 制約（PK に必要）
# MAGIC ALTER TABLE stores ALTER COLUMN store_id SET NOT NULL;
# MAGIC ALTER TABLE categories ALTER COLUMN category_id SET NOT NULL;
# MAGIC ALTER TABLE members ALTER COLUMN member_id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PK 制約
# MAGIC ALTER TABLE stores ADD CONSTRAINT pk_stores PRIMARY KEY (store_id);
# MAGIC ALTER TABLE categories ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);
# MAGIC ALTER TABLE members ADD CONSTRAINT pk_members PRIMARY KEY (member_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FK 制約
# MAGIC ALTER TABLE sales_daily ADD CONSTRAINT fk_sales_daily_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE sales_by_category ADD CONSTRAINT fk_sbc_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE sales_by_category ADD CONSTRAINT fk_sbc_category FOREIGN KEY (category_id) REFERENCES categories(category_id);
# MAGIC ALTER TABLE prescription_monthly ADD CONSTRAINT fk_rx_store FOREIGN KEY (store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE members ADD CONSTRAINT fk_members_store FOREIGN KEY (home_store_id) REFERENCES stores(store_id);
# MAGIC ALTER TABLE member_visits ADD CONSTRAINT fk_visits_member FOREIGN KEY (member_id) REFERENCES members(member_id);
# MAGIC ALTER TABLE member_visits ADD CONSTRAINT fk_visits_store FOREIGN KEY (store_id) REFERENCES stores(store_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. タグ付与

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">タグの活用</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Unity Catalog のタグを使うと、テーブルやカラムを<strong>分類・検索・ガバナンス</strong>に活用できます。<br/>
# MAGIC         Catalog Explorer でのフィルタリングや、データ分類ポリシーの適用に使えます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルタグ（ワークスペースのタグポリシーに合わせて値を変更してください）
# MAGIC ALTER TABLE stores SET TAGS ('source' = 'demo', 'owner' = 'store_ops');
# MAGIC ALTER TABLE categories SET TAGS ('source' = 'demo');
# MAGIC ALTER TABLE sales_daily SET TAGS ('source' = 'demo', 'grain' = 'daily');
# MAGIC ALTER TABLE sales_by_category SET TAGS ('source' = 'demo', 'grain' = 'monthly');
# MAGIC ALTER TABLE prescription_monthly SET TAGS ('source' = 'demo', 'grain' = 'monthly');
# MAGIC ALTER TABLE members SET TAGS ('source' = 'demo', 'pii' = 'true');
# MAGIC ALTER TABLE member_visits SET TAGS ('source' = 'demo', 'grain' = 'monthly');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PII カラムタグ（会員データ）
# MAGIC ALTER TABLE members ALTER COLUMN member_id SET TAGS ('pii' = 'identifier');
# MAGIC ALTER TABLE members ALTER COLUMN age_group SET TAGS ('pii' = 'demographic');
# MAGIC ALTER TABLE members ALTER COLUMN gender SET TAGS ('pii' = 'demographic');

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">データ作成完了</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         7テーブルの作成、メタデータ付与、PK/FK制約、タグ付与が完了しました。<br/>
# MAGIC         次のノートブックでは、このデータを使って「KPI定義がバラバラだとどうなるか」を体験します。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 00. 設定ノートブック]($./00_config) | [Next → 02. 従来のやり方の課題]($./02_従来のやり方の課題) |
