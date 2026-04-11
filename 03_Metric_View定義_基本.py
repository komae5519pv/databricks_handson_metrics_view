# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">03. Metric View 定義（基本）</span>
# MAGIC       <span style="background:#FF3621;color:#fff;padding:3px 12px;border-radius:12px;font-size:11px;font-weight:600;">Public Preview</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;">⏱ 25分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 3 of 6</span>
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
# MAGIC         Metric View を YAML で定義し、KPIを一元管理する方法を学ぶ。<br/>
# MAGIC         ディメンション・メジャーの定義パターンと、<code>FILTER</code> 付きメジャーによる柔軟な指標設計を体験します。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Metric View とは？

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin:16px 0;">
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #FF3621;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">🗄️</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Source（ベーステーブル）</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">集計対象のファクトテーブルを指定。<br/><code style="background:#f5f5f5;padding:2px 6px;border-radius:3px;font-size:12px;">source: sales_daily</code></div>
# MAGIC   </div>
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #7B61FF;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">🔀</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Dimensions（切り口）</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">GROUP BY の候補を定義。<br/>月別・曜日別・地域別など自由に選択可能</div>
# MAGIC   </div>
# MAGIC   <div style="border:1px solid #E0E0E0;border-radius:8px;padding:20px;border-top:3px solid #FF6F00;text-align:center;">
# MAGIC     <div style="font-size:28px;margin-bottom:8px;">📊</div>
# MAGIC     <div style="font-weight:700;font-size:15px;margin-bottom:6px;">Measures（指標）</div>
# MAGIC     <div style="font-size:13px;color:#666;line-height:1.5;">集計ロジック（SUM, AVG 等）を定義。<br/>クエリ時に <code style="background:#f5f5f5;padding:2px 6px;border-radius:3px;font-size:12px;">MEASURE()</code> で呼び出す</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         従来のViewは <code>GROUP BY</code> が固定されており、切り口を変えるたびに新しいViewが必要でした。<br/>
# MAGIC         <strong>Metric View は定義だけして、クエリ時に自由に GROUP BY できる</strong>のが最大の違いです。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:24px 0 12px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">Metric View のアーキテクチャ</span>
# MAGIC </div>
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC   subgraph Sources["ソーステーブル"]
# MAGIC     direction TB
# MAGIC     S1["sales_daily<br/>（日別売上）"]
# MAGIC     S2["sales_by_category<br/>（カテゴリ別売上）"]
# MAGIC     S3["stores<br/>（店舗マスタ）"]
# MAGIC     S4["categories<br/>（カテゴリマスタ）"]
# MAGIC   end
# MAGIC   MV["Metric View<br/>統一KPI定義<br/>（セマンティックレイヤー）"]
# MAGIC   MSG["利用者は<br/>統一されたKPI定義をクエリし<br/>一貫した数字を取得"]
# MAGIC   subgraph Consumers["利用者"]
# MAGIC     direction TB
# MAGIC     C1["Dashboard<br/>経営帳票"]
# MAGIC     C2["Genie<br/>自然言語分析"]
# MAGIC     C3["Notebook / SQL<br/>アドホック分析"]
# MAGIC   end
# MAGIC   S1 --> MV
# MAGIC   S2 --> MV
# MAGIC   S3 --> MV
# MAGIC   S4 --> MV
# MAGIC   MV --> MSG
# MAGIC   MSG --> C1
# MAGIC   MSG --> C2
# MAGIC   MSG --> C3
# MAGIC   style Sources fill:#e3f2fd,stroke:#1B5162,stroke-width:2px
# MAGIC   style Consumers fill:#EEF2F7,stroke:#64748B,stroke-width:1.25px
# MAGIC   style MV fill:#FFFFFF,stroke:#FF3621,stroke-width:2px
# MAGIC   style MSG fill:#FFFFFF,stroke:#FF3621,stroke-width:1px
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "neutral" });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 基本の Metric View を作成する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   日別売上テーブル <code>sales_daily</code> をソースとして、売上日・月・曜日のディメンションと、売上合計・客数・客単価などのメジャーを定義します。
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:24px 0 12px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">📖 YAML 構造の読み方</span>
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">フィールド</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">必須</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">意味</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">version</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">YAML仕様バージョン。現在は <code>1.1</code> 固定</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">source</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">集計元のテーブル名。ここが「ファクトテーブル」になる</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">filter</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">-</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">全クエリに適用されるデフォルト条件（WHERE句相当）</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">dimensions</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;"><strong>切り口</strong>の一覧。GROUP BY の候補になるカラムや式を定義</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">measures</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">✅</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;"><strong>集計指標</strong>の一覧。SUM / COUNT / AVG 等の集計ロジックを定義</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-weight:600;">joins</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">-</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">他テーブルとの結合定義（セクション4で使用）</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="font-size:13px;color:#666;margin:8px 0 4px;">各 dimension / measure には以下のプロパティを設定できます：</div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:8px 0 16px;font-size:13px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">プロパティ</th>
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">必須</th>
# MAGIC       <th style="padding:8px 12px;text-align:left;font-weight:600;border-bottom:2px solid #E0E0E0;">意味</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">name</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">✅</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">識別名。MEASURE() やクエリで使う名前</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">expr</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">✅</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">SQL式。カラム参照・関数・CASE WHEN 等</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">type</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">✅</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">データ型（string / date / bigint / double 等）</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">display_name</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">UIやGenieに表示する名前（日本語OK）</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">synonyms</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">Genieが認識する別名のリスト（日英両方推奨）</td></tr>
# MAGIC     <tr><td style="padding:6px 12px;border-bottom:1px solid #eee;font-family:monospace;">format</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">-</td><td style="padding:6px 12px;border-bottom:1px solid #eee;">表示フォーマット（小数桁数、通貨など）</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <hr style="border:none;border-top:2px solid #E0E0E0;margin:28px 0;"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Catalog Explorer + Genie Code で作成する（メイン）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ここでやること</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Catalog Explorer から Genie Code に<strong>日本語で指示</strong>を出して Metric View を作成します。<br/>
# MAGIC         コードを書く必要はありません。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#FF3621,#7B61FF);border-radius:2px;"></div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF3621;border:3px solid #fff;box-shadow:0 0 0 2px #FF3621;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 1: Catalog Explorer で <code>sales_daily</code> テーブルを開く</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">左サイドバーの <strong>Catalog</strong> → カタログ → スキーマ → <code>sales_daily</code> を選択</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF6F00;border:3px solid #fff;box-shadow:0 0 0 2px #FF6F00;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 2: Create &gt; Metric view をクリック</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">テーブル詳細ページの右上にある <strong>Create</strong> ボタンから <strong>Metric view</strong> を選択</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#009688;border:3px solid #fff;box-shadow:0 0 0 2px #009688;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 3: 名前とカタログ・スキーマを指定</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         <strong>Name:</strong> <code>drugstore_kpi_metrics</code><br/>
# MAGIC         <strong>Catalog:</strong> 00_config で設定したカタログ名<br/>
# MAGIC         <strong>Schema:</strong> 00_config で設定したスキーマ名<br/>
# MAGIC         → <strong>Create</strong> をクリック
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 4: Genie Code に以下の指示を入力</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">エディタ右上の Genie Code アイコンをクリックし、以下の日本語プロンプトを入力してください</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示（コピーして貼り付け）</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     日別売上データから、店舗の業績分析に使える Metric View を作ってください。<br/><br/>
# MAGIC     <strong>分析の切り口：</strong><br/>
# MAGIC     ・売上日（日付そのまま）<br/>
# MAGIC     ・売上月（月単位でまとめる）<br/>
# MAGIC     ・売上年（年単位でまとめる）<br/>
# MAGIC     ・曜日（日本語の曜日名で表示したい）<br/><br/>
# MAGIC     <strong>集計したい指標：</strong><br/>
# MAGIC     ・売上合計<br/>
# MAGIC     ・客数合計<br/>
# MAGIC     ・レシート枚数合計<br/>
# MAGIC     ・客単価（売上合計 ÷ 客数合計）<br/>
# MAGIC     ・客単価レシートベース（売上合計 ÷ レシート枚数合計）<br/><br/>
# MAGIC     <strong>その他：</strong><br/>
# MAGIC     ・表示名は日本語にしてください<br/>
# MAGIC     ・Genieで「売上」「revenue」どちらでも検索できるように同義語を設定してください<br/>
# MAGIC     ・対象期間は2024年1月1日以降にしてください
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#AB47BC,#4caf50);border-radius:2px;"></div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 5: 生成された YAML をレビュー</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">Genie Code が YAML を生成します。下の「正解例」と見比べて、意図通りか確認してください</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#4caf50;border:3px solid #fff;box-shadow:0 0 0 2px #4caf50;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 6: 保存</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">右上の <strong>Save</strong> をクリックして Metric View を作成します</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#fff3e0;cursor:pointer;font-weight:600;font-size:14px;color:#FF6F00;">📋 正解例: 期待される YAML（クリックで展開）</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <div style="font-size:13px;color:#555;margin-bottom:8px;">Catalog Explorer → Create → Metric view → 以下をエディタに貼り付け<br/><strong>※ source のカタログ名・スキーマ名は、ご自身の環境に合わせて読み替えてください</strong></div>
# MAGIC <button onclick="copyYaml1()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="yaml-block-1" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: 1.1
# MAGIC source: komae_demo_v4.drugstore_kpi.sales_daily
# MAGIC filter: sales_date >= '2021-01-01'
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: 売上日
# MAGIC     expr: sales_date
# MAGIC     comment: 売上が発生した日付
# MAGIC     display_name: 売上日
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC     synonyms:
# MAGIC       - 日付
# MAGIC       - sales date
# MAGIC       - date
# MAGIC   - name: 売上月
# MAGIC     expr: "DATE_TRUNC('MONTH', sales_date)"
# MAGIC     comment: 売上日を月単位に丸めた日付
# MAGIC     display_name: 売上月
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC     synonyms:
# MAGIC       - 月
# MAGIC       - 年月
# MAGIC       - sales month
# MAGIC       - month
# MAGIC   - name: 売上年
# MAGIC     expr: "DATE_TRUNC('YEAR', sales_date)"
# MAGIC     comment: 売上日を年単位に丸めた日付
# MAGIC     display_name: 売上年
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC     synonyms:
# MAGIC       - 年
# MAGIC       - sales year
# MAGIC       - year
# MAGIC   - name: 曜日
# MAGIC     expr: |-
# MAGIC       CASE dayofweek(sales_date)
# MAGIC         WHEN 1 THEN '日曜日'
# MAGIC         WHEN 2 THEN '月曜日'
# MAGIC         WHEN 3 THEN '火曜日'
# MAGIC         WHEN 4 THEN '水曜日'
# MAGIC         WHEN 5 THEN '木曜日'
# MAGIC         WHEN 6 THEN '金曜日'
# MAGIC         WHEN 7 THEN '土曜日'
# MAGIC       END
# MAGIC     comment: 売上日の曜日（日本語表記）
# MAGIC     display_name: 曜日
# MAGIC     synonyms:
# MAGIC       - day of week
# MAGIC       - weekday
# MAGIC
# MAGIC measures:
# MAGIC   - name: 売上合計
# MAGIC     expr: SUM(sales_amount)
# MAGIC     comment: 売上金額の合計（円）
# MAGIC     display_name: 売上合計
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - 売上
# MAGIC       - 総売上
# MAGIC       - total sales
# MAGIC       - revenue
# MAGIC   - name: 客数合計
# MAGIC     expr: SUM(customer_count)
# MAGIC     comment: ユニーク来店者数の合計
# MAGIC     display_name: 客数合計
# MAGIC     synonyms:
# MAGIC       - 客数
# MAGIC       - 来店者数
# MAGIC       - total customers
# MAGIC       - customer count
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: レシート枚数合計
# MAGIC     expr: SUM(receipt_count)
# MAGIC     comment: レシート枚数の合計
# MAGIC     display_name: レシート枚数合計
# MAGIC     synonyms:
# MAGIC       - レシート数
# MAGIC       - 会計数
# MAGIC       - total receipts
# MAGIC       - receipt count
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 客単価_客数ベース
# MAGIC     expr: MEASURE(`売上合計`) / MEASURE(`客数合計`)
# MAGIC     comment: 売上合計を客数合計で割った1人あたりの平均売上金額
# MAGIC     display_name: 客単価（客数ベース）
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - 客単価
# MAGIC       - 一人当たり売上
# MAGIC       - revenue per customer
# MAGIC       - average spend per customer
# MAGIC   - name: 客単価_レシートベース
# MAGIC     expr: MEASURE(`売上合計`) / MEASURE(`レシート枚数合計`)
# MAGIC     comment: 売上合計をレシート枚数合計で割った1会計あたりの平均売上金額
# MAGIC     display_name: 客単価（レシートベース）
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC     synonyms:
# MAGIC       - 会計単価
# MAGIC       - 一会計あたり売上
# MAGIC       - revenue per receipt
# MAGIC       - average transaction value
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyYaml1() {
# MAGIC   var el = document.getElementById("yaml-block-1");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopy1(text); });
# MAGIC   } else { fallbackCopy1(text); }
# MAGIC }
# MAGIC function fallbackCopy1(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. SQL で作成する場合（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">SQL での作成はオプションです</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         上の 2a で Catalog Explorer から作成済みの場合、このセルの実行は不要です。<br/>
# MAGIC         再現性が必要な場合や Git 管理したい場合に SQL での作成が便利です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# -- オプション: SQL で Metric View を作成（2a で作成済みの場合は実行不要）
spark.sql(f"""
CREATE OR REPLACE VIEW drugstore_kpi_metrics WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
source: {catalog_name}.{schema_name}.sales_daily
filter: sales_date >= '2021-01-01'

dimensions:
  - name: 売上日
    expr: sales_date
    comment: 売上が発生した日付
    display_name: 売上日
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - 日付
      - sales date
      - date
  - name: 売上月
    expr: "DATE_TRUNC('MONTH', sales_date)"
    comment: 売上日を月単位に丸めた日付
    display_name: 売上月
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - 月
      - 年月
      - sales month
      - month
  - name: 売上年
    expr: "DATE_TRUNC('YEAR', sales_date)"
    comment: 売上日を年単位に丸めた日付
    display_name: 売上年
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - 年
      - sales year
      - year
  - name: 曜日
    expr: |-
      CASE dayofweek(sales_date)
        WHEN 1 THEN '日曜日'
        WHEN 2 THEN '月曜日'
        WHEN 3 THEN '火曜日'
        WHEN 4 THEN '水曜日'
        WHEN 5 THEN '木曜日'
        WHEN 6 THEN '金曜日'
        WHEN 7 THEN '土曜日'
      END
    comment: 売上日の曜日（日本語表記）
    display_name: 曜日
    synonyms:
      - day of week
      - weekday

measures:
  - name: 売上合計
    expr: SUM(sales_amount)
    comment: 売上金額の合計（円）
    display_name: 売上合計
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - 売上
      - 総売上
      - total sales
      - revenue
  - name: 客数合計
    expr: SUM(customer_count)
    comment: ユニーク来店者数の合計
    display_name: 客数合計
    synonyms:
      - 客数
      - 来店者数
      - total customers
      - customer count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: レシート枚数合計
    expr: SUM(receipt_count)
    comment: レシート枚数の合計
    display_name: レシート枚数合計
    synonyms:
      - レシート数
      - 会計数
      - total receipts
      - receipt count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 客単価_客数ベース
    expr: MEASURE(`売上合計`) / MEASURE(`客数合計`)
    comment: 売上合計を客数合計で割った1人あたりの平均売上金額
    display_name: 客単価（客数ベース）
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - 客単価
      - 一人当たり売上
      - revenue per customer
      - average spend per customer
  - name: 客単価_レシートベース
    expr: MEASURE(`売上合計`) / MEASURE(`レシート枚数合計`)
    comment: 売上合計をレシート枚数合計で割った1会計あたりの平均売上金額
    display_name: 客単価（レシートベース）
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - 会計単価
      - 一会計あたり売上
      - revenue per receipt
      - average transaction value
$$
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">実行結果</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>drugstore_kpi_metrics</code> が Metric View として作成されました。<br/>
# MAGIC         4つのディメンション（売上日・月・年・曜日）と 5つのメジャー（売上合計・客数合計・レシート枚数・客単価2種）が定義されています。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. ディメンション定義パターン解説

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">パターン</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">説明</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">YAML 例</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:600;">① そのまま</span>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">カラムをそのまま使用</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">expr: sales_date</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <span style="background:#fff3e0;color:#e65100;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:600;">② SQL式変換</span>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>CASE WHEN</code> で値を変換<br/>（例: 曜日番号→日本語名）</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">expr: case when dayofweek(...) = 1 then '日曜' ...</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <span style="background:#e0f2f1;color:#00695c;padding:3px 10px;border-radius:4px;font-size:12px;font-weight:600;">③ 関数変換</span>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><code>DATE_TRUNC</code> 等で粒度を変換<br/>（例: 日付→月/年）</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">expr: DATE_TRUNC('MONTH', sales_date)</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <details style="margin:12px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC   <summary style="padding:12px 16px;background:#F8F9FA;cursor:pointer;font-weight:600;font-size:14px;">
# MAGIC     📖 補足: YAML の <code>expr</code> で使える SQL 関数
# MAGIC   </summary>
# MAGIC   <div style="padding:16px;font-size:14px;line-height:1.6;">
# MAGIC     <code>expr</code> には Databricks SQL で利用可能なすべてのスカラー関数が使えます。<br/>
# MAGIC     <code>DATE_TRUNC</code>, <code>CASE WHEN</code>, <code>CONCAT</code>, <code>COALESCE</code>, <code>ROUND</code> など、
# MAGIC     通常の SELECT 句で書けるものはすべて利用可能です。
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. FILTER 付きメジャーを追加する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   02 で体験した「調剤込み/抜き」問題を、<code>FILTER</code> 付きメジャーで解決します。<br/>
# MAGIC   カテゴリ別売上テーブルに店舗マスタ・カテゴリマスタを <code>JOIN</code> し、調剤売上・物販売上・食品構成比などの派生メジャーを定義します。
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>MEASURE(...) FILTER (WHERE ...)</code> 構文を使うと、既存のメジャーにフィルタ条件を付けた派生メジャーを定義できます。<br/>
# MAGIC         同じベースメジャーから「調剤のみ」「物販のみ」「食品のみ」を簡潔に分離できます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Catalog Explorer + Genie Code で作成する（メイン）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   セクション2と同様に、Catalog Explorer で <code>sales_by_category</code> テーブルを開き、<strong>Create > Metric view</strong> で新しい Metric View を作成します。
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:100px 1fr;gap:1px;background:#E0E0E0;border-radius:8px;overflow:hidden;margin:12px 0;font-size:14px;">
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Name</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;font-family:monospace;">drugstore_store_metrics</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Catalog</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したカタログ名</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:10px 14px;font-weight:600;">Schema</div>
# MAGIC   <div style="background:#fff;padding:10px 14px;">00_config で設定したスキーマ名</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;margin:12px 0;">
# MAGIC   Create をクリック後、Genie Code に以下の指示を入力してください。
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示（コピーして貼り付け）</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     カテゴリ別月次売上データから、店舗情報とカテゴリ情報を紐付けて、店舗×カテゴリの分析ができる Metric View を作ってください。<br/><br/>
# MAGIC     <strong>紐付けるテーブル：</strong><br/>
# MAGIC     ・stores テーブル（店舗マスタ）を store_id で結合<br/>
# MAGIC     ・categories テーブル（カテゴリマスタ）を category_id で結合<br/><br/>
# MAGIC     <strong>分析の切り口：</strong><br/>
# MAGIC     ・売上月<br/>
# MAGIC     ・店舗名、地域、店舗タイプ<br/>
# MAGIC     ・調剤の有無（「調剤あり」「調剤なし」で表示）<br/>
# MAGIC     ・カテゴリ名<br/><br/>
# MAGIC     <strong>集計したい指標：</strong><br/>
# MAGIC     ・カテゴリ別売上（合計）<br/>
# MAGIC     ・調剤売上（カテゴリが「調剤」のものだけ抽出）<br/>
# MAGIC     ・物販売上（調剤以外の売上）<br/>
# MAGIC     ・食品売上（カテゴリが「食品・飲料」のものだけ抽出）<br/>
# MAGIC     ・食品構成比（食品売上 ÷ 物販売上 × 100、パーセント表示）<br/><br/>
# MAGIC     <strong>その他：</strong><br/>
# MAGIC     ・表示名は日本語、Genieで日英どちらでも検索できるように同義語を設定してください
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:24px 0 12px;">
# MAGIC   <span style="font-size:16px;font-weight:700;color:#1B3139;">📖 この Metric View の新しいポイント</span>
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">機能</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">YAMLでの書き方</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">何ができるか</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">joins</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">joins:<br/>  - name: stores<br/>    source: stores<br/>    on: source.store_id = stores.store_id</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">他テーブルの列をディメンションに使える</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">FILTER付きメジャー</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">expr: MEASURE(`カテゴリ別売上`)<br/>  FILTER (WHERE ... = '調剤')</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">既存メジャーに条件を付けた派生指標</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">Composable<br/>（メジャー参照）</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">expr: MEASURE(`食品売上`) * 100.0<br/>  / MEASURE(`物販売上`)</td>
# MAGIC       <td style="padding:8px 16px;border-bottom:1px solid #E0E0E0;">メジャー同士を組み合わせた複合指標</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#fff3e0;cursor:pointer;font-weight:600;font-size:14px;color:#FF6F00;">📋 正解例: 期待される YAML（クリックで展開）</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <div style="font-size:13px;color:#555;margin-bottom:8px;">Catalog Explorer → Create → Metric view → 以下をエディタに貼り付け<br/><strong>※ source のカタログ名・スキーマ名は、ご自身の環境に合わせて読み替えてください</strong></div>
# MAGIC <button onclick="copyYaml2()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="yaml-block-2" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: '1.1'
# MAGIC source: komae_demo_v4.drugstore_kpi.sales_by_category
# MAGIC comment: カテゴリ別月次売上データに店舗・カテゴリマスタを結合し、店舗×カテゴリの多軸分析を行うための Metric View
# MAGIC joins:
# MAGIC   - name: stores
# MAGIC     source: komae_demo_v4.drugstore_kpi.stores
# MAGIC     on: source.store_id = stores.store_id
# MAGIC   - name: categories
# MAGIC     source: komae_demo_v4.drugstore_kpi.categories
# MAGIC     on: source.category_id = categories.category_id
# MAGIC dimensions:
# MAGIC   - name: 売上月
# MAGIC     expr: source.month
# MAGIC     display_name: 売上月
# MAGIC     comment: 対象年月（月初日）
# MAGIC     synonyms:
# MAGIC       - sales month
# MAGIC       - month
# MAGIC       - 月
# MAGIC       - 対象月
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC   - name: 店舗名
# MAGIC     expr: stores.store_name
# MAGIC     display_name: 店舗名
# MAGIC     comment: 店舗名
# MAGIC     synonyms:
# MAGIC       - store name
# MAGIC       - 店名
# MAGIC       - store
# MAGIC   - name: 地域
# MAGIC     expr: stores.region
# MAGIC     display_name: 地域
# MAGIC     comment: 地域（北海道/東北/関東/中部/東海/関西/中国/四国/九州）
# MAGIC     synonyms:
# MAGIC       - region
# MAGIC       - エリア
# MAGIC       - area
# MAGIC   - name: 店舗タイプ
# MAGIC     expr: stores.store_type
# MAGIC     display_name: 店舗タイプ
# MAGIC     comment: 店舗タイプ（駅前型/郊外型/ロードサイド型）
# MAGIC     synonyms:
# MAGIC       - store type
# MAGIC       - 店舗形態
# MAGIC       - type
# MAGIC   - name: 調剤の有無
# MAGIC     expr: CASE WHEN stores.has_pharmacy THEN '調剤あり' ELSE '調剤なし' END
# MAGIC     display_name: 調剤の有無
# MAGIC     comment: 店舗の調剤薬局併設有無（調剤あり/調剤なし）
# MAGIC     synonyms:
# MAGIC       - pharmacy availability
# MAGIC       - has pharmacy
# MAGIC       - 調剤併設
# MAGIC   - name: カテゴリ名
# MAGIC     expr: categories.category_name
# MAGIC     display_name: カテゴリ名
# MAGIC     comment: 商品カテゴリ名（医薬品/調剤/化粧品/日用品/食品・飲料/サプリメント/介護）
# MAGIC     synonyms:
# MAGIC       - category name
# MAGIC       - category
# MAGIC       - カテゴリ
# MAGIC       - 商品カテゴリ
# MAGIC measures:
# MAGIC   - name: カテゴリ別売上
# MAGIC     expr: SUM(source.sales_amount)
# MAGIC     display_name: カテゴリ別売上
# MAGIC     comment: カテゴリ別の売上金額合計（円）
# MAGIC     synonyms:
# MAGIC       - category sales
# MAGIC       - total sales
# MAGIC       - 売上合計
# MAGIC       - sales amount
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 調剤売上
# MAGIC     expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` = '調剤')"
# MAGIC     display_name: 調剤売上
# MAGIC     comment: 調剤カテゴリの売上金額合計（円）
# MAGIC     synonyms:
# MAGIC       - pharmacy sales
# MAGIC       - dispensing sales
# MAGIC       - 調剤
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 物販売上
# MAGIC     expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` != '調剤')"
# MAGIC     display_name: 物販売上
# MAGIC     comment: 調剤以外の売上金額合計（円）
# MAGIC     synonyms:
# MAGIC       - retail sales
# MAGIC       - non-pharmacy sales
# MAGIC       - merchandise sales
# MAGIC       - 物販
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 食品売上
# MAGIC     expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` = '食品・飲料')"
# MAGIC     display_name: 食品売上
# MAGIC     comment: 食品・飲料カテゴリの売上金額合計（円）
# MAGIC     synonyms:
# MAGIC       - food sales
# MAGIC       - food and beverage sales
# MAGIC       - 食品
# MAGIC       - 食品・飲料売上
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC   - name: 食品構成比
# MAGIC     expr: "MEASURE(`食品売上`) / MEASURE(`物販売上`)"
# MAGIC     display_name: 食品構成比
# MAGIC     comment: 物販売上に占める食品売上の割合（%）
# MAGIC     synonyms:
# MAGIC       - food ratio
# MAGIC       - food share
# MAGIC       - food percentage
# MAGIC       - 食品比率
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyYaml2() {
# MAGIC   var el = document.getElementById("yaml-block-2");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopy2(text); });
# MAGIC   } else { fallbackCopy2(text); }
# MAGIC }
# MAGIC function fallbackCopy2(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. SQL で作成する場合（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #607d8b;background:#eceff1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚙️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">SQL での作成はオプションです</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         上の 3a で Catalog Explorer から作成済みの場合、このセルの実行は不要です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# -- オプション: SQL で Metric View を作成（3a で作成済みの場合は実行不要）
spark.sql(f"""
CREATE OR REPLACE VIEW drugstore_store_metrics WITH METRICS LANGUAGE YAML AS
$$
version: '1.1'
source: {catalog_name}.{schema_name}.sales_by_category
comment: カテゴリ別月次売上データに店舗・カテゴリマスタを結合し、店舗×カテゴリの多軸分析を行うための Metric View
joins:
  - name: stores
    source: {catalog_name}.{schema_name}.stores
    on: source.store_id = stores.store_id
  - name: categories
    source: {catalog_name}.{schema_name}.categories
    on: source.category_id = categories.category_id
dimensions:
  - name: 売上月
    expr: source.month
    display_name: 売上月
    comment: 対象年月（月初日）
    synonyms:
      - sales month
      - month
      - 月
      - 対象月
    format:
      type: date
      date_format: year_month_day
  - name: 店舗名
    expr: stores.store_name
    display_name: 店舗名
    comment: 店舗名
    synonyms:
      - store name
      - 店名
      - store
  - name: 地域
    expr: stores.region
    display_name: 地域
    comment: 地域（北海道/東北/関東/中部/東海/関西/中国/四国/九州）
    synonyms:
      - region
      - エリア
      - area
  - name: 店舗タイプ
    expr: stores.store_type
    display_name: 店舗タイプ
    comment: 店舗タイプ（駅前型/郊外型/ロードサイド型）
    synonyms:
      - store type
      - 店舗形態
      - type
  - name: 調剤の有無
    expr: CASE WHEN stores.has_pharmacy THEN '調剤あり' ELSE '調剤なし' END
    display_name: 調剤の有無
    comment: 店舗の調剤薬局併設有無（調剤あり/調剤なし）
    synonyms:
      - pharmacy availability
      - has pharmacy
      - 調剤併設
  - name: カテゴリ名
    expr: categories.category_name
    display_name: カテゴリ名
    comment: 商品カテゴリ名（医薬品/調剤/化粧品/日用品/食品・飲料/サプリメント/介護）
    synonyms:
      - category name
      - category
      - カテゴリ
      - 商品カテゴリ
measures:
  - name: カテゴリ別売上
    expr: SUM(source.sales_amount)
    display_name: カテゴリ別売上
    comment: カテゴリ別の売上金額合計（円）
    synonyms:
      - category sales
      - total sales
      - 売上合計
      - sales amount
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 調剤売上
    expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` = '調剤')"
    display_name: 調剤売上
    comment: 調剤カテゴリの売上金額合計（円）
    synonyms:
      - pharmacy sales
      - dispensing sales
      - 調剤
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 物販売上
    expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` != '調剤')"
    display_name: 物販売上
    comment: 調剤以外の売上金額合計（円）
    synonyms:
      - retail sales
      - non-pharmacy sales
      - merchandise sales
      - 物販
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 食品売上
    expr: "SUM(source.sales_amount) FILTER (WHERE `カテゴリ名` = '食品・飲料')"
    display_name: 食品売上
    comment: 食品・飲料カテゴリの売上金額合計（円）
    synonyms:
      - food sales
      - food and beverage sales
      - 食品
      - 食品・飲料売上
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: 食品構成比
    expr: "MEASURE(`食品売上`) / MEASURE(`物販売上`)"
    display_name: 食品構成比
    comment: 物販売上に占める食品売上の割合（%）
    synonyms:
      - food ratio
      - food share
      - food percentage
      - 食品比率
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
$$
""")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">実行結果</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>drugstore_store_metrics</code> が作成されました。<br/>
# MAGIC         <code>FILTER</code> 付きメジャーにより、カテゴリ別売上から調剤売上・物販売上・食品売上を派生定義しています。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agent Metadata の効果

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.6;margin:12px 0;">
# MAGIC   Metric View の YAML には、集計ロジックだけでなく <strong>AI エージェント向けのメタデータ</strong> を埋め込めます。<br/>
# MAGIC   これにより Genie や AI/BI Dashboard がメトリクスを正しく理解し、自然言語で問い合わせできるようになります。
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">プロパティ</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">用途</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">例</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">
# MAGIC         <code>display_name</code>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">UI に表示される名前。日本語で分かりやすく</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">display_name: '客単価'</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">
# MAGIC         <code>synonyms</code>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">Genie が認識する別名リスト</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">synonyms: ['売上', 'revenue', 'sales']</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">
# MAGIC         <code>format</code>
# MAGIC       </td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">数値フォーマット（小数桁数等）</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">
# MAGIC         <code style="background:#1e1e1e;color:#d4d4d4;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:13px;">format: { type: number, decimal_places: { type: exact, places: 0 } }</code>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <code>synonyms</code> を設定しておくと、Genie で「売上」「revenue」どちらでもヒットするようになります。<br/>
# MAGIC         日本語と英語の両方を登録しておくのがおすすめです。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">このノートブックで学んだこと</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <strong>2つの Metric View を作成しました：</strong>
# MAGIC         <ul style="margin:8px 0 0;padding-left:20px;">
# MAGIC           <li><code>drugstore_kpi_metrics</code> — 日別売上の基本KPI（売上合計・客数・客単価など）</li>
# MAGIC           <li><code>drugstore_store_metrics</code> — カテゴリ別×店舗のKPI（FILTER付き派生メジャー）</li>
# MAGIC         </ul>
# MAGIC         <div style="margin-top:8px;">
# MAGIC           次のノートブックでは、これらの Metric View を <code>MEASURE()</code> でクエリし、自由な切り口で集計する方法を体験します。
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 02. 従来のやり方の課題]($./02_従来のやり方の課題) | [Next → 04. Metric View を使ってみる]($./04_Metric_Viewを使ってみる) |
