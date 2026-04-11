# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 24px 32px; border-radius: 12px; margin-bottom: 24px;">
# MAGIC   <div style="display: flex; align-items: center; gap: 16px;">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="40" height="40" alt="Databricks"/>
# MAGIC     <div>
# MAGIC       <div style="color: #ffffff; font-size: 28px; font-weight: 700; letter-spacing: -0.5px;">02. 従来のやり方の課題</div>
# MAGIC       <div style="color: rgba(255,255,255,0.75); font-size: 14px; margin-top: 4px;">UC Business Semantics ハンズオン — Step 2 of 6</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #ffc107; background: #fffde7; padding: 16px 20px; border-radius: 8px; margin-bottom: 20px;">
# MAGIC   <div style="font-weight: 700; color: #ffc107; margin-bottom: 6px;">🎯 Goal</div>
# MAGIC   <div style="color: #333;">KPI 定義がバラバラだとどうなるか体験する。同じ指標名でも、定義が違えば数字が変わる——この"メトリクスドリフト"を実際に SQL で確かめましょう。</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 「客単価」の定義ブレ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC     「客単価」は誰もが使う KPI ですが、部門ごとに計算方法が異なることがあります。以下の 3 パターンを比較してみましょう。
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin:16px 0;">
# MAGIC   <div style="border:2px solid #1976d2;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">A: 営業部方式</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">売上 ÷ <span style="color:#1976d2;">来客数</span></div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #FF6F00;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">B: マーケ部方式</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">売上 ÷ <span style="color:#FF6F00;">レシート枚数</span></div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #7b1fa2;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">C: 経理部方式</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;"><span style="color:#7b1fa2;">月次カテゴリ売上</span> ÷ 来客数</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="text-align:center;color:#999;font-size:13px;margin-bottom:8px;">↓ 同じ「客単価」なのに、分母（分子）が違う ↓</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;">
# MAGIC   <div style="font-weight: 700; color: #009688;">Definition A — 営業部方式</div>
# MAGIC   <div style="color: #555; font-size: 14px;">客単価 = 売上合計 ÷ 来客数合計</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Definition A: 営業部方式
# MAGIC -- 客単価 = SUM(sales_amount) / SUM(customer_count)
# MAGIC SELECT
# MAGIC   '営業部方式' AS definition,
# MAGIC   ROUND(SUM(sales_amount) / SUM(customer_count), 1) AS avg_spend_per_customer
# MAGIC FROM sales_daily

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;">
# MAGIC   <div style="font-weight: 700; color: #009688;">Definition B — マーケ部方式</div>
# MAGIC   <div style="color: #555; font-size: 14px;">客単価 = 売上合計 ÷ レシート枚数合計（＝買上点数ベース）</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Definition B: マーケ部方式
# MAGIC -- 客単価 = SUM(sales_amount) / SUM(receipt_count)
# MAGIC SELECT
# MAGIC   'マーケ部方式' AS definition,
# MAGIC   ROUND(SUM(sales_amount) / SUM(receipt_count), 1) AS avg_spend_per_customer
# MAGIC FROM sales_daily

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;">
# MAGIC   <div style="font-weight: 700; color: #009688;">Definition C — 経理部方式</div>
# MAGIC   <div style="color: #555; font-size: 14px;">客単価 = カテゴリ別月次売上合計 ÷ 来客数（月次集計ベース）</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Definition C: 経理部方式
# MAGIC -- 月次カテゴリ別売上合計 / customer_count
# MAGIC SELECT
# MAGIC   '経理部方式' AS definition,
# MAGIC   ROUND(SUM(sc.sales_amount) / SUM(sd.customer_count), 1) AS avg_spend_per_customer
# MAGIC FROM sales_by_category sc
# MAGIC JOIN (
# MAGIC   SELECT
# MAGIC     store_id,
# MAGIC     DATE_TRUNC('month', sales_date) AS month,
# MAGIC     SUM(customer_count) AS customer_count
# MAGIC   FROM sales_daily
# MAGIC   GROUP BY store_id, DATE_TRUNC('month', sales_date)
# MAGIC ) sd
# MAGIC   ON sc.store_id = sd.store_id
# MAGIC   AND sc.month = sd.month

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 8px; margin-bottom: 20px;">
# MAGIC   <div style="font-weight: 700; color: #f44336; margin-bottom: 6px;">❌ Error — 同じ「客単価」なのに数字が違う！</div>
# MAGIC   <div style="color: #333;">
# MAGIC     3 つの定義すべてが「客単価」を名乗っていますが、<strong>分母が異なる</strong>ため結果がバラバラです。<br/>
# MAGIC     レポートを受け取った経営層は「どの数字が正しいのか？」と混乱し、数字の突合に貴重な時間が費やされます。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 「調剤込みの売上」問題

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC     ドラッグストアでは「調剤を含めるか否か」で売上の分母が大きく変わります。<br/>食品構成比を例に、分母の違いが KPI に与える影響を見てみましょう。
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0;">
# MAGIC   <div style="border:2px solid #1976d2;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">A: 全売上ベース</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">食品売上 ÷ <span style="color:#1976d2;">全売上（調剤込み）</span></div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #FF6F00;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">B: 物販ベース</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">食品売上 ÷ <span style="color:#FF6F00;">物販売上（調剤抜き）</span></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="text-align:center;color:#999;font-size:13px;margin-bottom:8px;">↓ 分子は同じ。分母が違うだけで構成比が変わる ↓</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;">
# MAGIC   <div style="font-weight: 700; color: #009688;">Query A — 全売上（調剤込み）で食品構成比を計算</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query A: 全売上（調剤込み）ベースの食品構成比
# MAGIC WITH total AS (
# MAGIC   SELECT SUM(sales_amount) AS total_sales
# MAGIC   FROM sales_by_category
# MAGIC ),
# MAGIC food AS (
# MAGIC   SELECT SUM(sc.sales_amount) AS food_sales
# MAGIC   FROM sales_by_category sc
# MAGIC   JOIN categories c ON sc.category_id = c.category_id
# MAGIC   WHERE c.category_name = '食品・飲料'
# MAGIC )
# MAGIC SELECT
# MAGIC   '全売上（調剤込み）' AS base,
# MAGIC   food.food_sales,
# MAGIC   total.total_sales,
# MAGIC   ROUND(food.food_sales / total.total_sales * 100, 2) AS food_ratio_pct
# MAGIC FROM total, food

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #009688; background: #e0f2f1; padding: 12px 16px; border-radius: 8px; margin-bottom: 8px;">
# MAGIC   <div style="font-weight: 700; color: #009688;">Query B — 物販のみ（調剤抜き）で食品構成比を計算</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query B: 物販のみ（調剤抜き）ベースの食品構成比
# MAGIC WITH retail_total AS (
# MAGIC   SELECT SUM(sc.sales_amount) AS total_sales
# MAGIC   FROM sales_by_category sc
# MAGIC   JOIN categories c ON sc.category_id = c.category_id
# MAGIC   WHERE c.category_name != '調剤'
# MAGIC ),
# MAGIC food AS (
# MAGIC   SELECT SUM(sc.sales_amount) AS food_sales
# MAGIC   FROM sales_by_category sc
# MAGIC   JOIN categories c ON sc.category_id = c.category_id
# MAGIC   WHERE c.category_name = '食品・飲料'
# MAGIC )
# MAGIC SELECT
# MAGIC   '物販のみ（調剤抜き）' AS base,
# MAGIC   food.food_sales,
# MAGIC   retail_total.total_sales,
# MAGIC   ROUND(food.food_sales / retail_total.total_sales * 100, 2) AS food_ratio_pct
# MAGIC FROM retail_total, food

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 16px 20px; border-radius: 8px; margin-bottom: 20px;">
# MAGIC   <div style="font-weight: 700; color: #ff9800; margin-bottom: 6px;">⚠️ Warning — 分母が違えば構成比も変わる。どちらが正しい？</div>
# MAGIC   <div style="color: #333;">
# MAGIC     調剤を含めるかどうかで食品構成比が数ポイント変わります。<br/>
# MAGIC     「どちらの定義が正しいか」ではなく、<strong>「どの文脈でどの定義を使うか」を組織で統一する</strong>ことが重要です。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 「既存店前年比」の定義ブレ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC     ドラッグストア業界では、新店のオープン景気を除いた<strong>「既存店売上高前年比」</strong>が IR の最重要 KPI です。<br/>
# MAGIC     しかし「既存店」の定義は部署によって異なります。この「線引き」の違いだけで前年比が変わることを確かめましょう。
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0;">
# MAGIC   <div style="border:2px solid #1976d2;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">A: 営業部方式</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">既存店 = 開店から<span style="color:#1976d2;font-size:18px;"> 1年以上</span></div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #FF6F00;border-radius:8px;padding:16px;text-align:center;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;">B: 経営企画部方式</div>
# MAGIC     <div style="font-size:14px;font-weight:700;color:#1B3139;margin-top:6px;">既存店 = 開店から<span style="color:#FF6F00;font-size:18px;"> 2年以上</span></div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="text-align:center;color:#999;font-size:13px;margin-bottom:8px;">↓ 「既存店」に含まれる店舗数が変わる → 前年比も変わる ↓</div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;padding:12px 16px;border-radius:8px;margin-bottom:8px;">
# MAGIC   <div style="font-weight:700;color:#009688;">Query A — 営業部方式: 既存店 = 開店から1年以上</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 定義A: 既存店 = 開店から1年以上（2024年1月以前に開店）
# MAGIC WITH existing_stores AS (
# MAGIC   SELECT store_id
# MAGIC   FROM stores
# MAGIC   WHERE open_date < '2024-01-01'
# MAGIC ),
# MAGIC cy AS (
# MAGIC   SELECT SUM(sd.sales_amount) AS sales
# MAGIC   FROM sales_daily sd
# MAGIC   JOIN existing_stores es ON sd.store_id = es.store_id
# MAGIC   WHERE YEAR(sd.sales_date) = 2025
# MAGIC ),
# MAGIC py AS (
# MAGIC   SELECT SUM(sd.sales_amount) AS sales
# MAGIC   FROM sales_daily sd
# MAGIC   JOIN existing_stores es ON sd.store_id = es.store_id
# MAGIC   WHERE YEAR(sd.sales_date) = 2024
# MAGIC )
# MAGIC SELECT
# MAGIC   '営業部方式（開店1年以上）' AS definition,
# MAGIC   cy.sales AS cy_sales_2025,
# MAGIC   py.sales AS py_sales_2024,
# MAGIC   ROUND(cy.sales / py.sales * 100, 1) AS yoy_pct
# MAGIC FROM cy, py

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;padding:12px 16px;border-radius:8px;margin-bottom:8px;">
# MAGIC   <div style="font-weight:700;color:#009688;">Query B — 経営企画部方式: 既存店 = 開店から2年以上</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 定義B: 既存店 = 開店から2年以上（2023年1月以前に開店）
# MAGIC WITH existing_stores AS (
# MAGIC   SELECT store_id
# MAGIC   FROM stores
# MAGIC   WHERE open_date < '2023-01-01'
# MAGIC ),
# MAGIC cy AS (
# MAGIC   SELECT SUM(sd.sales_amount) AS sales
# MAGIC   FROM sales_daily sd
# MAGIC   JOIN existing_stores es ON sd.store_id = es.store_id
# MAGIC   WHERE YEAR(sd.sales_date) = 2025
# MAGIC ),
# MAGIC py AS (
# MAGIC   SELECT SUM(sd.sales_amount) AS sales
# MAGIC   FROM sales_daily sd
# MAGIC   JOIN existing_stores es ON sd.store_id = es.store_id
# MAGIC   WHERE YEAR(sd.sales_date) = 2024
# MAGIC )
# MAGIC SELECT
# MAGIC   '経営企画部方式（開店2年以上）' AS definition,
# MAGIC   cy.sales AS cy_sales_2025,
# MAGIC   py.sales AS py_sales_2024,
# MAGIC   ROUND(cy.sales / py.sales * 100, 1) AS yoy_pct
# MAGIC FROM cy, py

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #f44336;background:#ffebee;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">❌</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">同じ「既存店前年比」なのに、定義で数字が違う！</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         「既存店」を<strong>開店1年以上</strong>にするか<strong>2年以上</strong>にするかで、含まれる店舗が変わり、前年比も変わります。<br/>
# MAGIC         経営会議で「既存店前年比は○%です」と報告しても、営業部と経営企画部で数字が食い違えば議論になりません。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #7b1fa2;background:#f3e5f5;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">実務ではもっと複雑です</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ここでは「開店1年 vs 2年」で比較しましたが、実際にはさらに細かい定義の差が生じます：
# MAGIC         <ul style="margin:8px 0 0 16px;">
# MAGIC           <li><strong>改装した店舗</strong> → 既存店のまま？ 改装後は新店扱い？</li>
# MAGIC           <li><strong>業態変更</strong> → 調剤を新設した店舗は既存店？ 新店？</li>
# MAGIC           <li><strong>移転した店舗</strong> → 近距離なら既存店？ 移転先は新店？</li>
# MAGIC           <li><strong>閉店予定の店舗</strong> → 含める？ 除外？</li>
# MAGIC         </ul>
# MAGIC         重要なのは「1年か2年か」ではなく、<strong>どの定義を使うかが組織で統一されていない限り、数字がズレる</strong>という点です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ — Before / After

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0 28px 0;">
# MAGIC
# MAGIC   <!-- Before Card -->
# MAGIC   <div style="background: #fff; border: 2px solid #f44336; border-radius: 12px; overflow: hidden;">
# MAGIC     <div style="background: #f44336; color: #fff; padding: 12px 16px; font-weight: 700; font-size: 16px;">😰 Before（現状）</div>
# MAGIC     <div style="padding: 16px;">
# MAGIC       <ul style="margin: 0; padding-left: 20px; color: #333; line-height: 1.8;">
# MAGIC         <li>KPI 定義が部門ごとにバラバラ</li>
# MAGIC         <li>同じ名前の指標なのに数字が合わない</li>
# MAGIC         <li><strong>メトリクスドリフト</strong>が常態化</li>
# MAGIC         <li>数字の突合・原因調査に膨大な時間</li>
# MAGIC         <li>経営判断のスピードが落ちる</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- After Card -->
# MAGIC   <div style="background: #fff; border: 2px solid #4caf50; border-radius: 12px; overflow: hidden;">
# MAGIC     <div style="background: #4caf50; color: #fff; padding: 12px 16px; font-weight: 700; font-size: 16px;">🚀 After（Metric View で解決）</div>
# MAGIC     <div style="padding: 16px;">
# MAGIC       <ul style="margin: 0; padding-left: 20px; color: #333; line-height: 1.8;">
# MAGIC         <li>Unity Catalog に<strong>統一定義</strong>を格納</li>
# MAGIC         <li>全員が同じメトリクスを参照</li>
# MAGIC         <li>定義変更はバージョン管理・ガバナンス付き</li>
# MAGIC         <li>BI ツール・SQL・AI が同じ数字を返す</li>
# MAGIC         <li>突合作業ゼロ → 分析にフォーカス</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #4caf50; background: #e8f5e9; padding: 16px 20px; border-radius: 8px; margin-bottom: 20px;">
# MAGIC   <div style="font-weight: 700; color: #4caf50; margin-bottom: 6px;">✅ Next Step</div>
# MAGIC   <div style="color: #333;">次のノートブックでは、<strong>Metric View</strong> を使ってこれらの KPI を統一定義します。メトリクスドリフトのない世界を体験しましょう！</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 01. サンプルデータ作成]($./01_サンプルデータ作成) | [Next → 03. Metric View 定義（基本）]($./03_Metric_View定義_基本) |
