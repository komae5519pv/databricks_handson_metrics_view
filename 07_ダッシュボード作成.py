# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">07. ダッシュボード作成</span>
# MAGIC       <span style="background:#1976d2;color:#fff;padding:3px 12px;border-radius:12px;font-size:11px;font-weight:600;">AI/BI Dashboard</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;">⏱ 25分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 7 of 7</span>
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
# MAGIC         Genie Code のエージェントモードを使って、<strong>Metric View ベースの経営 KPI ダッシュボード</strong>を作成する。<br/>
# MAGIC         Metric View で定義した指標をそのままダッシュボードに反映し、Genie スペースと<strong>同じ数字</strong>が表示されることを確認する。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Genie Code でダッシュボードを作る利点</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         AI/BI ダッシュボードは手動でも作成できますが、Genie Code を使うと：
# MAGIC         <ul style="margin:8px 0 0 16px;">
# MAGIC           <li>自然言語で「こんなダッシュボードが欲しい」と伝えるだけで作れる</li>
# MAGIC           <li>Metric View の MEASURE() を使ったデータセットを自動生成してくれる</li>
# MAGIC           <li>チャートの種類やレイアウトも提案してくれる</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 完成イメージ
# MAGIC
# MAGIC ![](./_images/dashboard.png)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,プロンプトで使うカタログ・スキーマ名
print(f"Genie Code のプロンプト内では、以下のパスを指定してください：")
print(f"  {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 事前準備

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ff9800;background:#fff3e0;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚠️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">前提条件</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         以下が完了していることを確認してください：
# MAGIC         <ul style="margin:8px 0 0 16px;">
# MAGIC           <li><strong>03</strong> で <code>drugstore_kpi_metrics</code>（日別売上 Metric View）を作成済み</li>
# MAGIC           <li><strong>03</strong> で <code>drugstore_store_metrics</code>（カテゴリ別×店舗 Metric View）を作成済み</li>
# MAGIC           <li><strong>06</strong> で Genie スペースを作成済み（ダッシュボードへの埋め込みに使用）</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ダッシュボードを作成する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#FF3621,#7B61FF);border-radius:2px;"></div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF3621;border:3px solid #fff;box-shadow:0 0 0 2px #FF3621;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 1: 新しいダッシュボードを作成</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         左サイドバーの <strong>Dashboards</strong> → 右上の <strong>Create dashboard</strong> をクリック<br/>
# MAGIC         タイトルを <strong>「店舗経営KPIレポート」</strong> に変更（左上のタイトル部分をクリックして編集）
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF6F00;border:3px solid #fff;box-shadow:0 0 0 2px #FF6F00;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 2: Genie Code を開く</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         ダッシュボード編集画面の右上にある <strong>Genie Code</strong> ボタンをクリック（↓このアイコンです）
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 3: 以下のプロンプトを入力</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         Genie Code に日本語で指示を出します。下のプロンプトをコピーして貼り付けてください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#4caf50;border:3px solid #fff;box-shadow:0 0 0 2px #4caf50;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 4: 生成結果を確認して保存</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         Genie Code がデータセット・チャート・レイアウトを自動生成します。確認して <strong>Save</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC **Genie Code アイコン:** <img src="./_images/geniecode.png" width="40"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Genie Code へのプロンプト

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         一度に完璧なダッシュボードを作ろうとせず、<strong>ページごとに分けて指示</strong>するのがコツです。<br/>
# MAGIC         まず全社サマリを作り、その後「ページを追加して」と依頼していきます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. ページ1: 全社サマリ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #ff9800;background:#fff3e0;border-radius:8px;padding:12px 20px;margin:16px 0;">
# MAGIC   <div style="font-size:14px;color:#333;">⚠️ 下のプロンプト内の <code>カタログ名.スキーマ名</code> は、上のセルで表示されたパスに置き換えてから貼り付けてください。</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示（コピーして貼り付け）</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     以下の Metric View を使って、ドラッグストアの経営 KPI ダッシュボードを作ってください。<br/><br/>
# MAGIC     <strong>使う Metric View:</strong><br/>
# MAGIC     ・<code>カタログ名.スキーマ名.drugstore_kpi_metrics</code>（日別売上の基本KPI）<br/>
# MAGIC     ・<code>カタログ名.スキーマ名.drugstore_store_metrics</code>（カテゴリ別×店舗のKPI）<br/><br/>
# MAGIC     <strong>ページ1「全社サマリ」に欲しいもの:</strong><br/>
# MAGIC     ・売上合計、客数合計、客単価、食品構成比の KPI スコアカード（大きい数字で目立つように、前月比較を付けて）<br/>
# MAGIC     ・月次売上推移の折れ線グラフ<br/>
# MAGIC     ・カテゴリ別売上の円グラフまたは棒グラフ<br/>
# MAGIC     ・地域別売上の横棒グラフ<br/><br/>
# MAGIC     データセットはすべて MEASURE() を使って Metric View から取得してください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. ページ2: 店舗パフォーマンス

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ページの追加方法</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ページ1が完成したら、Genie Code に続けて以下を入力してください。<br/>
# MAGIC         「新しいページを追加して」と言えば、既存のダッシュボードにページが追加されます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示（コピーして貼り付け）</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     新しいページ「店舗パフォーマンス」を追加してください。<br/><br/>
# MAGIC     <strong>欲しいもの:</strong><br/>
# MAGIC     ・店舗別の売上・客数・客単価・食品構成比の一覧テーブル<br/>
# MAGIC     ・調剤ありの店舗と無しの店舗で売上を比較するグラフ<br/>
# MAGIC     ・店舗タイプ別（駅前型/郊外型/ロードサイド型）の売上比較<br/><br/>
# MAGIC     データセットは <code>カタログ名.スキーマ名.drugstore_store_metrics</code> の MEASURE() を使ってください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. ページ3: 調剤・カテゴリ分析（オプション）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border:2px solid #AB47BC;border-radius:8px;padding:16px 20px;margin:16px 0;background:#faf5ff;">
# MAGIC   <div style="font-weight:700;color:#AB47BC;font-size:13px;margin-bottom:8px;">💬 Genie Code への指示（コピーして貼り付け）</div>
# MAGIC   <div style="font-size:14px;color:#1B3139;line-height:1.8;background:#fff;border:1px solid #e0d4f5;border-radius:6px;padding:12px 16px;">
# MAGIC     新しいページ「調剤・カテゴリ分析」を追加してください。<br/><br/>
# MAGIC     <strong>欲しいもの:</strong><br/>
# MAGIC     ・カテゴリ別の月次売上推移（折れ線グラフ、カテゴリごとに色分け）<br/>
# MAGIC     ・調剤売上と物販売上の月次推移を並べて比較<br/>
# MAGIC     ・食品構成比の月次推移（フード＆ドラッグ戦略の進捗を可視化）<br/><br/>
# MAGIC     データセットは <code>カタログ名.スキーマ名.drugstore_store_metrics</code> の MEASURE() を使ってください。
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ダッシュボードを調整する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="font-size:14px;color:#333;line-height:1.8;margin:12px 0;">
# MAGIC   Genie Code が生成したダッシュボードは、そのままでも使えますが、以下の調整をするとより良くなります。
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:16px 0;">
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-weight:700;font-size:14px;margin-bottom:6px;">📐 レイアウト調整</div>
# MAGIC     <div style="font-size:13px;color:#555;line-height:1.6;">
# MAGIC       ウィジェットのドラッグ＆ドロップでサイズや位置を調整できます。KPI カードは上部に横並び、グラフはその下に配置するのが定番です。
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-weight:700;font-size:14px;margin-bottom:6px;">🎨 チャートの変更</div>
# MAGIC     <div style="font-size:13px;color:#555;line-height:1.6;">
# MAGIC       各ウィジェットをクリックしてチャートの種類を変更できます。Genie Code に「この棒グラフを円グラフにして」と指示することもできます。
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-weight:700;font-size:14px;margin-bottom:6px;">🔍 フィルタ追加</div>
# MAGIC     <div style="font-size:13px;color:#555;line-height:1.6;">
# MAGIC       Genie Code に「期間フィルタと地域フィルタを追加して」と指示すると、ダッシュボード上部にフィルタが追加されます。
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-weight:700;font-size:14px;margin-bottom:6px;">📊 Genie 埋め込み</div>
# MAGIC     <div style="font-size:13px;color:#555;line-height:1.6;">
# MAGIC       06 で作成した Genie スペースをダッシュボードに埋め込めます。手順は次のセクションで説明します。
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Genie スペースをダッシュボードに埋め込む

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">なぜ埋め込むのか？</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ダッシュボードは「定型帳票」、Genie は「アドホック分析」。<br/>
# MAGIC         両方を1つの画面に並べると、帳票で気になった数字をその場で深掘りできます。
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
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 1: Settings and themes を開く</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">ダッシュボード右上のケバブメニュー（⋮）から <strong>Settings and themes</strong> をクリック</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF6F00;border:3px solid #fff;box-shadow:0 0 0 2px #FF6F00;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 2: Genie space リンク欄を確認</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">設定 → 一般 → <strong>「既存の Genie space をリンク」</strong> 欄を確認</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#AB47BC;border:3px solid #fff;box-shadow:0 0 0 2px #AB47BC;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 3: Genie スペースのリンクをコピー</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">06 で作成した Genie スペースを開き、右上の <strong>「共有」</strong> からリンクをコピー</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#4caf50;border:3px solid #fff;box-shadow:0 0 0 2px #4caf50;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 4: リンクを貼り付け</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">ダッシュボードの設定に戻り、コピーした Genie space のリンクを貼り付けて保存</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Genie ウィジェットは新しいページに配置するのもおすすめです。<br/>
# MAGIC         Genie Code に「新しいページを追加して、Genie スペースを全画面で埋め込んで」と指示することもできます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 操作イメージ

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1:** ダッシュボード右上のケバブメニュー（⋮）から「Settings and themes」をクリック
# MAGIC
# MAGIC ![](./_images/add_genie_to_dashboard_1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:** 設定 → 一般 → 「既存の Genie space をリンク」を確認
# MAGIC
# MAGIC ![](./_images/add_genie_to_dashboard_2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3:** 06 で作成した Genie スペースを開き、右上の「共有」からリンクをコピー
# MAGIC
# MAGIC ![](./_images/add_genie_to_dashboard_3.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4:** ダッシュボードの設定に戻り、コピーした Genie space のリンクを貼り付け
# MAGIC
# MAGIC ![](./_images/add_genie_to_dashboard_4.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Genie スペースとの一致を確認する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0;">
# MAGIC   <div style="border:2px solid #1976d2;border-radius:8px;padding:20px;background:#E3F2FD;">
# MAGIC     <div style="font-size:13px;font-weight:700;color:#1976d2;margin-bottom:8px;">📊 ダッシュボード</div>
# MAGIC     <div style="font-size:14px;line-height:1.6;">
# MAGIC       全社サマリの KPI カード<br/>
# MAGIC       → 売上合計: <strong>XXX 億円</strong>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #7B61FF;border-radius:8px;padding:20px;background:#F3E5F5;">
# MAGIC     <div style="font-size:13px;font-weight:700;color:#7B61FF;margin-bottom:8px;">🤖 Genie スペース</div>
# MAGIC     <div style="font-size:14px;line-height:1.6;">
# MAGIC       「全社の売上合計は？」と質問<br/>
# MAGIC       → 売上合計: <strong>XXX 億円</strong>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">数字が一致！</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         ダッシュボードも Genie スペースも<strong>同じ Metric View を参照</strong>しているため、数字は完全に一致します。<br/>
# MAGIC         経営帳票と現場の分析で数字がズレることは、もうありません。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. まとめ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ハンズオン完了！</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.8;">
# MAGIC         このハンズオンで体験したこと：<br/>
# MAGIC         <strong>02.</strong> KPI 定義がバラバラだと数字がズレる問題を体験<br/>
# MAGIC         <strong>03.</strong> <strong style="color:#AB47BC;">Genie Code</strong> で Metric View を作成し、KPI を一元定義<br/>
# MAGIC         <strong>04.</strong> MEASURE() で自在にクエリ<br/>
# MAGIC         <strong>05.</strong> Composable / Window の応用<br/>
# MAGIC         <strong>06.</strong> Genie スペースで現場が自然言語で分析<br/>
# MAGIC         <strong>07.</strong> <strong style="color:#AB47BC;">Genie Code</strong> で経営KPIダッシュボードを自動作成<br/><br/>
# MAGIC         <strong style="color:#FF3621;">キーメッセージ:</strong><br/>
# MAGIC         <strong>Metric View</strong> で KPI の定義を一元管理すれば、誰が・どのツールで分析しても、同じ数字になる。<br/>
# MAGIC         そして <strong>Genie Code</strong> が、その定義作成もダッシュボード構築も自然言語でサポートしてくれる。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 06. Genie スペース作成]($./06_Genieスペース作成) | [🏠 00. 設定ノートブック]($./00_config) |
