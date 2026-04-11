# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">00. 設定ノートブック</span>
# MAGIC       <span style="background:#607d8b;color:#fff;padding:3px 12px;border-radius:12px;font-size:11px;font-weight:600;">Config</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 0 of 6</span>
# MAGIC   </div>
# MAGIC   <img src="https://cdn.simpleicons.org/databricks/FF3621" width="36" height="36"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">設定</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         このノートブックは全ハンズオンで共通使用する<strong>カタログ・スキーマ</strong>を設定します。<br/>
# MAGIC         <strong>サーバレスコンピュート</strong>または<strong>SQL Warehouse</strong>で実行してください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,変数設定
catalog_name = "komae_demo_v4"          # 任意のカタログ名に変更してください
schema_name = "drugstore_kpi"           # 任意のスキーマ名に変更してください

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")

# COMMAND ----------

# DBTITLE 1,カタログ・スキーマ作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog_name};")
spark.sql(f"USE SCHEMA {schema_name};")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print(f"catalog: {catalog_name}")
print(f"schema: {schema_name}")
print(f"実行済み: USE CATALOG {catalog_name}")
print(f"実行済み: USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">設定完了</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         以降のノートブックでは <code>%run ./00_config</code> でこの設定を読み込みます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | | [Next → 01. サンプルデータ作成]($./01_サンプルデータ作成) |
