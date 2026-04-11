# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">05. Metric View 定義（応用）</span>
# MAGIC       <span style="background:#FF6F00;color:#fff;padding:3px 12px;border-radius:12px;font-size:11px;font-weight:600;">応用</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;">⏱ 15分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 5 of 6</span>
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
# MAGIC         Star JOIN、Composable Measures、Window Measures、semiadditive を体験する
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Composable Measures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>MEASURE()</code> 参照で既存の指標を組み合わせて新しい指標を作れる。計算ロジックの重複を防ぐ
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW drugstore_advanced_metrics WITH METRICS LANGUAGE YAML AS
$$
version: '1.1'
source: {catalog_name}.{schema_name}.sales_daily
comment: 日別売上に店舗マスタを結合し、Composable Measures で客単価・坪効率を定義する応用 Metric View

joins:
  - name: stores
    source: {catalog_name}.{schema_name}.stores
    on: source.store_id = stores.store_id

dimensions:
  - name: 売上月
    expr: "DATE_TRUNC('MONTH', source.sales_date)"
    display_name: 売上月
    comment: 売上日を月単位に丸めた日付
    synonyms:
      - 月
      - month
  - name: 店舗名
    expr: stores.store_name
    display_name: 店舗名
    comment: 店舗名
    synonyms:
      - 店舗
      - store
  - name: 地域
    expr: stores.region
    display_name: 地域
    comment: 地域（北海道/東北/関東/中部/東海/関西/中国/四国/九州）
    synonyms:
      - region
      - エリア
  - name: 調剤の有無
    expr: CASE WHEN stores.has_pharmacy THEN '調剤あり' ELSE '調剤なし' END
    display_name: 調剤の有無
    comment: 店舗の調剤薬局併設有無
    synonyms:
      - 調剤併設
      - pharmacy
  - name: 売場面積
    expr: stores.size_sqm
    display_name: 売場面積（㎡）
    comment: 店舗の売場面積（平方メートル）

measures:
  - name: 売上合計
    expr: SUM(source.sales_amount)
    display_name: 売上合計
    comment: 売上金額の合計（円）
    synonyms:
      - 売上
      - revenue
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 客数合計
    expr: SUM(source.customer_count)
    display_name: 客数合計
    comment: 来店客数の合計
    synonyms:
      - 客数
      - customers
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 客単価
    expr: "MEASURE(`売上合計`) / MEASURE(`客数合計`)"
    display_name: 客単価
    comment: 1人あたりの平均売上金額
    synonyms:
      - average spend
      - 一人当たり売上
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 坪効率
    expr: "MEASURE(`売上合計`) / SUM(stores.size_sqm) * 3.3"
    display_name: 坪効率（円/坪）
    comment: 坪あたりの売上金額（売場面積を坪に換算）
    synonyms:
      - 坪単価
      - sales per tsubo
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 店舗数
    expr: COUNT(DISTINCT source.store_id)
    display_name: 店舗数
    comment: ユニーク店舗数
$$
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `店舗名`, MEASURE(`売上合計`), MEASURE(`客数合計`), MEASURE(`客単価`), MEASURE(`坪効率`)
# MAGIC FROM drugstore_advanced_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY MEASURE(`坪効率`) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         客単価 = 売上合計 / 客数合計。<code>MEASURE()</code> 参照なので定義が1箇所。変更時も1箇所直せばOK
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Window Measures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ff9800;background:#fff3e0;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚠️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">注意</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Window Measures は Experimental です。本番利用には注意してください
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"""
ALTER VIEW drugstore_advanced_metrics AS
$$
version: '1.1'
source: {catalog_name}.{schema_name}.sales_daily
comment: 日別売上に店舗マスタを結合し、Composable Measures + Window Measures を定義する応用 Metric View

joins:
  - name: stores
    source: {catalog_name}.{schema_name}.stores
    on: source.store_id = stores.store_id

dimensions:
  - name: 売上日
    expr: source.sales_date
    display_name: 売上日
    comment: 売上が発生した日付
  - name: 売上月
    expr: "DATE_TRUNC('MONTH', source.sales_date)"
    display_name: 売上月
    comment: 売上日を月単位に丸めた日付
    synonyms:
      - 月
      - month
  - name: 店舗名
    expr: stores.store_name
    display_name: 店舗名
  - name: 地域
    expr: stores.region
    display_name: 地域
    synonyms:
      - region
      - エリア
  - name: 調剤の有無
    expr: CASE WHEN stores.has_pharmacy THEN '調剤あり' ELSE '調剤なし' END
    display_name: 調剤の有無
    comment: 店舗の調剤薬局併設有無
  - name: 売場面積
    expr: stores.size_sqm
    display_name: 売場面積（㎡）

measures:
  - name: 売上合計
    expr: SUM(source.sales_amount)
    display_name: 売上合計
    comment: 売上金額の合計（円）
    synonyms:
      - 売上
      - revenue
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 客数合計
    expr: SUM(source.customer_count)
    display_name: 客数合計
    comment: 来店客数の合計
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 客単価
    expr: "MEASURE(`売上合計`) / MEASURE(`客数合計`)"
    display_name: 客単価
    comment: 1人あたりの平均売上金額
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 坪効率
    expr: "MEASURE(`売上合計`) / SUM(stores.size_sqm) * 3.3"
    display_name: 坪効率（円/坪）
    comment: 坪あたりの売上金額
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 店舗数
    expr: COUNT(DISTINCT source.store_id)
    display_name: 店舗数
  - name: 7日移動平均売上
    expr: "MEASURE(`売上合計`)"
    display_name: 7日移動平均売上
    comment: 直近7日間の売上合計（移動平均）
    window:
      - order: 売上日
        range: trailing 7 day
        semiadditive: last
  - name: 累計売上
    expr: "MEASURE(`売上合計`)"
    display_name: 累計売上
    comment: 累計売上合計
    synonyms:
      - YTD売上
      - cumulative
    window:
      - order: 売上月
        range: cumulative
        semiadditive: last
$$
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `売上日`, MEASURE(`売上合計`), MEASURE(`7日移動平均売上`)
# MAGIC FROM drugstore_advanced_metrics
# MAGIC WHERE `売上日` >= '2025-11-01'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `売上日`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `売上月`, MEASURE(`売上合計`), MEASURE(`累計売上`)
# MAGIC FROM drugstore_advanced_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `売上月`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style="margin:12px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC   <summary style="padding:12px 16px;background:#F8F9FA;cursor:pointer;font-weight:600;font-size:14px;">
# MAGIC     📖 補足: semiadditive とは？
# MAGIC   </summary>
# MAGIC   <div style="padding:16px;font-size:14px;line-height:1.6;">
# MAGIC     <code>semiadditive: last</code> は、ウィンドウ内の「最後の値を取る」ことを意味します（合計ではなく）。<br/>
# MAGIC     例えば、7日移動平均では直近7日間の売上合計の最終値を返し、累計売上では期首からの累積値を返します。<br/>
# MAGIC     在庫残高や口座残高のように「足し合わせると意味がない」指標に適したセマンティクスです。
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materialization

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #7b1fa2;background:#f3e5f5;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Note</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Materialization は Experimental。ここでは概念の紹介のみ
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   Metric View の YAML に <code>materialization</code> セクションを追加すると、集計結果を事前計算できます。<br/>
# MAGIC   以下は設定例です（このノートブックでは実行しません）。
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#1e1e1e;border-radius:8px;padding:16px 20px;margin:16px 0;overflow-x:auto;">
# MAGIC <pre style="color:#d4d4d4;font-family:'Cascadia Code','Fira Code',monospace;font-size:13px;line-height:1.6;margin:0;"><span style="color:#569cd6;">materialization:</span>
# MAGIC   <span style="color:#569cd6;">schedule:</span> <span style="color:#ce9178;">every 6 hours</span>
# MAGIC   <span style="color:#569cd6;">mode:</span> <span style="color:#ce9178;">relaxed</span>
# MAGIC   <span style="color:#569cd6;">materialized_views:</span>
# MAGIC     - <span style="color:#569cd6;">name:</span> <span style="color:#ce9178;">daily_store_metrics</span>
# MAGIC       <span style="color:#569cd6;">type:</span> <span style="color:#ce9178;">aggregated</span>
# MAGIC       <span style="color:#569cd6;">dimensions:</span> <span style="color:#ce9178;">[売上日, 店舗名]</span>
# MAGIC       <span style="color:#569cd6;">measures:</span> <span style="color:#ce9178;">[売上合計, 客数合計]</span></pre>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Materialization を設定すると、集計結果が事前計算される。クエリは自動的に最適なパスにルーティングされる
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <hr style="border:none;border-top:2px solid #E0E0E0;margin:32px 0;"/>
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">まとめ: このノートブックで学んだこと</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <ul style="margin:8px 0 0;padding-left:20px;">
# MAGIC           <li><strong>Composable Measures</strong> <span style="background:#e8f5e9;color:#2e7d32;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;">GA</span> — <code>MEASURE()</code> 参照で指標を組み合わせ、定義の重複を排除</li>
# MAGIC           <li><strong>Window Measures: trailing, cumulative</strong> <span style="background:#ffebee;color:#c62828;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;">Experimental</span> — 移動平均・累計をYAMLで定義</li>
# MAGIC           <li><strong>semiadditive</strong> <span style="background:#ffebee;color:#c62828;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;">Experimental</span> — 合計ではなく最終値を取るセマンティクス</li>
# MAGIC           <li><strong>Materialization の概念紹介</strong> — 事前集計によるクエリ最適化</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 04. Metric View を使ってみる]($./04_Metric_Viewを使ってみる) | [Next → 06. Genie スペース作成]($./06_Genieスペース作成) |
