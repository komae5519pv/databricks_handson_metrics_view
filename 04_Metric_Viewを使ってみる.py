# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 20px;">
# MAGIC   <div style="display: flex; align-items: center; gap: 15px;">
# MAGIC     <img src="https://cdn.simpleicons.org/databricks/FF3621" width="40" height="40"/>
# MAGIC     <div>
# MAGIC       <h1 style="color: #FFFFFF; margin: 0; font-size: 28px;">UC Business Semantics ハンズオン</h1>
# MAGIC       <p style="color: #FFFFFFCC; margin: 5px 0 0 0; font-size: 16px;">Step 4 of 6 — 04. Metric View を使ってみる</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background: #fffde7; border-left: 5px solid #ffc107; padding: 15px 20px; border-radius: 6px; margin-bottom: 15px;">
# MAGIC   <h3 style="color: #ffc107; margin-top: 0;">🎯 Goal</h3>
# MAGIC   <p style="margin-bottom: 0;"><code>MEASURE()</code> で Metric View をクエリし、<strong>1つの定義であらゆる切り口の分析ができること</strong>、そして<strong>数字がブレないこと</strong>を体験する</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. MEASURE() の基本

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #e3f2fd; border-left: 5px solid #1976d2; padding: 15px 20px; border-radius: 6px; margin-bottom: 15px;">
# MAGIC   <h3 style="color: #1976d2; margin-top: 0;">ℹ️ Info</h3>
# MAGIC   <p style="margin-bottom: 0;">Metric View のメジャーは必ず <code>MEASURE()</code> 関数で呼び出します。<code>SELECT *</code> は使えません。<br/>
# MAGIC   クエリ時に <code>GROUP BY</code> で指定するディメンションに応じて、自動的に正しい集計が行われます。</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   `売上月`,
# MAGIC   MEASURE(`売上合計`),
# MAGIC   MEASURE(`客数合計`),
# MAGIC   MEASURE(`客単価_客数ベース`)
# MAGIC FROM drugstore_kpi_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY `売上月`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #e0f2f1; border-left: 5px solid #009688; padding: 15px 20px; border-radius: 6px; margin-bottom: 15px;">
# MAGIC   <h3 style="color: #009688; margin-top: 0;">💡 Tip</h3>
# MAGIC   <p style="margin-bottom: 0;"><code>MEASURE()</code> を使うと、集計ロジックを書く必要がありません。切り口（<code>GROUP BY</code>）だけ指定すれば、定義済みの計算が自動的に適用されます。</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 同じ定義を異なる切り口で

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <p>1つの Metric View から、ディメンションを変えるだけで様々な分析ができます。これが従来の View との最大の違いです。</p>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. 曜日別分析

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   `曜日`,
# MAGIC   MEASURE(`売上合計`),
# MAGIC   MEASURE(`客数合計`),
# MAGIC   MEASURE(`客単価_客数ベース`)
# MAGIC FROM drugstore_kpi_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY MEASURE(`売上合計`) DESC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #e0f2f1; border-left: 5px solid #009688; padding: 15px 20px; border-radius: 6px; margin-bottom: 15px;">
# MAGIC   <h3 style="color: #009688; margin-top: 0;">💡 Tip</h3>
# MAGIC   <p style="margin-bottom: 0;">同じ指標を別の切り口で見たいとき、Metric View なら <code>GROUP BY</code> を変えるだけ。従来なら SELECT 句の集計ロジックも全部書き直す必要がありました。</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. カテゴリ×店舗の Metric View を使う

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. 地域別売上

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style="margin-bottom: 15px;">
# MAGIC <summary style="cursor: pointer; font-weight: bold; color: #666; padding: 8px 12px; background: #f5f5f5; border-radius: 6px; border: 1px solid #ddd;">📝 Metric View なしだとこう書く（クリックで展開）</summary>
# MAGIC <div style="background: #f8f8f8; border: 1px solid #e0e0e0; border-radius: 0 0 6px 6px; padding: 15px; margin-top: -1px; font-family: monospace; font-size: 13px; white-space: pre; overflow-x: auto; color: #333;">-- Metric View なしの場合: JOIN + CASE WHEN + FILTER が必要
# MAGIC SELECT
# MAGIC   s.region AS 地域,
# MAGIC   SUM(sc.sales_amount) AS カテゴリ別売上,
# MAGIC   SUM(CASE WHEN c.category_name = '調剤'
# MAGIC            THEN sc.sales_amount ELSE 0 END) AS 調剤売上,
# MAGIC   SUM(CASE WHEN c.category_name != '調剤'
# MAGIC            THEN sc.sales_amount ELSE 0 END) AS 物販売上,
# MAGIC   SUM(CASE WHEN c.category_name = '食品'
# MAGIC            THEN sc.sales_amount ELSE 0 END)
# MAGIC   / NULLIF(
# MAGIC       SUM(CASE WHEN c.category_name != '調剤'
# MAGIC                THEN sc.sales_amount ELSE 0 END), 0
# MAGIC     ) AS 食品構成比
# MAGIC FROM sales_by_category sc
# MAGIC JOIN stores s ON sc.store_id = s.store_id
# MAGIC JOIN categories c ON sc.category_id = c.category_id
# MAGIC WHERE sc.sales_month >= '2024-01-01'
# MAGIC GROUP BY s.region
# MAGIC ORDER BY カテゴリ別売上 DESC</div>
# MAGIC </details>
# MAGIC
# MAGIC <div style="background:#e0f2f1;border-left:5px solid #009688;padding:15px 20px;border-radius:6px;margin-bottom:15px;">
# MAGIC   <div style="font-weight:700;color:#009688;margin-bottom:6px;">💡 上の 20行の SQL が、↓ の 7行になります</div>
# MAGIC   <div>JOIN も CASE WHEN も FILTER も、すべて Metric View の定義に入っているので、クエリする側は<strong>何を見たいか</strong>だけ書けばOKです。</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ↓ たった7行で、地域別の売上・調剤・物販・食品構成比がすべて出る
# MAGIC SELECT
# MAGIC   `地域`,
# MAGIC   MEASURE(`カテゴリ別売上`),
# MAGIC   MEASURE(`調剤売上`),
# MAGIC   MEASURE(`物販売上`),
# MAGIC   MEASURE(`食品構成比`)
# MAGIC FROM drugstore_store_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY MEASURE(`カテゴリ別売上`) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. 調剤の有無で比較

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   `調剤の有無`,
# MAGIC   MEASURE(`カテゴリ別売上`),
# MAGIC   MEASURE(`調剤売上`),
# MAGIC   MEASURE(`物販売上`),
# MAGIC   MEASURE(`食品構成比`)
# MAGIC FROM drugstore_store_metrics
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. まとめ: Metric View で何が変わったか

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin:20px 0;">
# MAGIC   <div style="border:2px solid #E0E0E0;border-radius:10px;padding:20px;background:#fafafa;">
# MAGIC     <div style="font-size:15px;font-weight:700;color:#999;margin-bottom:12px;">❌ Metric View がない世界</div>
# MAGIC     <div style="font-size:14px;line-height:1.8;color:#333;">
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #eee;">「客単価」の計算方法が人によって違い、会議で数字が合わない</div>
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #eee;">食品構成比の分母が曖昧で、レポートのたびに確認が必要</div>
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #eee;">切り口を変えるたびに JOIN や CASE WHEN を含む長いSQLを書き直す</div>
# MAGIC       <div style="padding:8px 0;color:#c62828;font-weight:600;">→ 分析の時間より、数字の突合と SQL の修正に時間がかかる</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #4caf50;border-radius:10px;padding:20px;background:#e8f5e9;">
# MAGIC     <div style="font-size:15px;font-weight:700;color:#2e7d32;margin-bottom:12px;">✅ Metric View がある世界</div>
# MAGIC     <div style="font-size:14px;line-height:1.8;color:#333;">
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #a5d6a7;">「客単価」の計算方法は定義済み。誰がクエリしても同じ数字</div>
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #a5d6a7;">食品構成比の分母は「物販売上」で固定。迷わない</div>
# MAGIC       <div style="padding:8px 0;border-bottom:1px solid #a5d6a7;">切り口を変えたいときは GROUP BY を変えるだけ（7行で完了）</div>
# MAGIC       <div style="padding:8px 0;color:#2e7d32;font-weight:600;">→ 分析そのものに集中できる</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックで体験したこと</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>MEASURE()</code> を使えば、定義済みのKPIを<strong>切り口だけ変えて</strong>自在に分析できます。<br/>
# MAGIC         そしてこの Metric View は、ダッシュボードや Genie からも参照されます。<br/>
# MAGIC         <strong>つまり、誰が・どのツールで分析しても、同じ数字が返ります。</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 定義を確認する

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED drugstore_kpi_metrics AS JSON

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #e0f2f1; border-left: 5px solid #009688; padding: 15px 20px; border-radius: 6px; margin-bottom: 15px;">
# MAGIC   <h3 style="color: #009688; margin-top: 0;">💡 Tip</h3>
# MAGIC   <p style="margin-bottom: 0;">YAML 定義の全体を確認できます。変更が必要な場合は <code>ALTER VIEW</code> で更新します。</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 03. Metric View 定義（基本）]($./03_Metric_View定義_基本) | [Next → 05. Metric View 定義（応用）]($./05_Metric_View定義_応用) |
