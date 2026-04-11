# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:linear-gradient(135deg,#1B3139 0%,#2D4A54 100%);border-radius:8px;margin-bottom:8px;">
# MAGIC   <div>
# MAGIC     <div style="display:flex;align-items:center;gap:10px;">
# MAGIC       <span style="color:#fff;font-size:22px;font-weight:700;">06. Genie スペース作成</span>
# MAGIC       <span style="background:#7B61FF;color:#fff;padding:3px 12px;border-radius:12px;font-size:11px;font-weight:600;">Genie Space</span>
# MAGIC       <span style="background:#e3f2fd;color:#1565c0;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;">⏱ 15分</span>
# MAGIC     </div>
# MAGIC     <span style="color:rgba(255,255,255,0.7);font-size:13px;">UC Business Semantics ハンズオン — Step 6 of 7</span>
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
# MAGIC         Metric View をデータソースにした <strong>Genie スペース</strong>を作成し、<br/>
# MAGIC         現場スタッフが<strong>自然言語で KPI を分析</strong>できる環境を構築する。<br/>
# MAGIC         ダッシュボードと<strong>同じ Metric View</strong> を使うため、数字が完全に一致することを確認する。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Genie スペースとは？</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Genie スペースは Databricks の<strong>自然言語 BI</strong> ツールです。<br/>
# MAGIC         ユーザーが「先月の売上は？」と質問するだけで、Genie が SQL を自動生成し、結果を返します。<br/>
# MAGIC         Metric View を接続すると、<strong>synonyms（同義語）</strong>や<strong>display_name</strong> を自動的に活用し、<br/>
# MAGIC         より正確な回答が可能になります。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Genie スペースを作成する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="position:relative;padding-left:36px;margin:16px 0;">
# MAGIC   <div style="position:absolute;left:14px;top:0;bottom:0;width:3px;background:linear-gradient(to bottom,#FF3621,#7B61FF);border-radius:2px;"></div>
# MAGIC
# MAGIC   <!-- Step 1 -->
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF3621;border:3px solid #fff;box-shadow:0 0 0 2px #FF3621;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 1: サイドバーから Genie を開く</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">左サイドバーの <strong>Genie</strong> をクリック → 右上の <strong>「New」</strong> をクリック</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Step 2 -->
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#FF6F00;border:3px solid #fff;box-shadow:0 0 0 2px #FF6F00;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 2: データソースを選択</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">
# MAGIC         <strong>Metric View</strong> と <strong>stores テーブル</strong> を選択します。<br/>
# MAGIC         ポイント: 生テーブルを大量に追加するのではなく、<strong>Metric View を中心に選ぶ</strong>ことで Genie の精度が上がります。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Step 3 -->
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#7B61FF;border:3px solid #fff;box-shadow:0 0 0 2px #7B61FF;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 3: タイトル・説明を設定</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">下記の設定内容を入力します</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Step 4 -->
# MAGIC   <div style="position:relative;margin-bottom:20px;">
# MAGIC     <div style="position:absolute;left:-30px;top:4px;width:12px;height:12px;border-radius:50%;background:#4caf50;border:3px solid #fff;box-shadow:0 0 0 2px #4caf50;"></div>
# MAGIC     <div style="background:#F8F9FA;border-radius:8px;padding:16px 20px;">
# MAGIC       <div style="font-weight:700;font-size:14px;">Step 4: 保存</div>
# MAGIC       <div style="font-size:13px;color:#555;margin-top:4px;">右上の <strong>「Save」</strong> をクリックして完了</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:28px 0 16px;">
# MAGIC   <span style="font-size:18px;font-weight:700;color:#1B3139;">📋 登録するデータソース</span>
# MAGIC </div>
# MAGIC
# MAGIC <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">種類</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">オブジェクト名</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;">説明</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#FF3621;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">Metric View</span></td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:13px;">drugstore_kpi_metrics</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">03 で作成した基本 Metric View（売上/客数/客単価/FILTER付き/Agent Metadata）</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#FF3621;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">Metric View</span></td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:13px;">drugstore_advanced_metrics</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">05 で作成した応用 Metric View（JOIN/composable/window）</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#1976d2;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">Table</span></td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:13px;">stores</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">店舗マスタ（店舗名、地域、調剤有無などの属性で絞り込み用）</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <div style="border-left:4px solid #009688;background:#e0f2f1;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">💡</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">ヒント: なぜ Metric View をデータソースにするのか？</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         <strong>生テーブルを大量登録（最大30テーブル）するのではなく、Metric View を使う理由：</strong>
# MAGIC         <ul style="margin:8px 0 0 20px;">
# MAGIC           <li>メトリクスの定義が <strong>YAML に集約済み</strong> → Genie が解釈しやすい</li>
# MAGIC           <li><strong>synonyms（同義語）</strong>が設定済み → 「売上」「revenue」どちらでもヒット</li>
# MAGIC           <li><strong>display_name / format</strong> → 結果の表示が自動で見やすくなる</li>
# MAGIC           <li>JOIN も Metric View 内で定義済み → テーブル間の関係を Genie に教える必要なし</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-bottom:2px solid #E0E0E0;padding-bottom:8px;margin:28px 0 16px;">
# MAGIC   <span style="font-size:18px;font-weight:700;color:#1B3139;">⚙️ 設定内容</span>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:140px 1fr;gap:1px;background:#E0E0E0;border-radius:8px;overflow:hidden;margin:16px 0;font-size:14px;">
# MAGIC   <div style="background:#1B3139;color:#fff;padding:12px 16px;font-weight:600;">Title</div>
# MAGIC   <div style="background:#fff;padding:12px 16px;font-family:monospace;">店舗KPI分析アシスタント</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:12px 16px;font-weight:600;">Description</div>
# MAGIC   <div style="background:#fff;padding:12px 16px;">ドラッグストアの店舗KPIを自然言語で分析。売上・客数・客単価・調剤実績・カテゴリ構成比を統一されたメトリクス定義に基づいて回答します。</div>
# MAGIC   <div style="background:#1B3139;color:#fff;padding:12px 16px;font-weight:600;">Warehouse</div>
# MAGIC   <div style="background:#fff;padding:12px 16px;">SQL Warehouse（サーバレス推奨）</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Instructions を設定する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #7b1fa2;background:#f3e5f5;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">📝</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Instructions の優先順位</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Genie は以下の優先順位で Instructions を参照します：<br/>
# MAGIC         <strong>① SQL Expressions</strong> ＞ <strong>② Example SQL Queries</strong> ＞ <strong>③ Text Instructions</strong><br/><br/>
# MAGIC         Metric View を使う場合、メトリクス定義自体が「SQL Expression」の役割を果たすため、<br/>
# MAGIC         Instructions は<strong>最小限でOK</strong>です。<br/><br/>
# MAGIC         <span style="font-size:12px;color:#666;">参考: <a href="https://docs.databricks.com/aws/en/genie/best-practices" target="_blank">Curate an effective Genie space | Databricks Docs</a></span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC Genie スペースの「Instructions」タブに以下を登録してください。
# MAGIC
# MAGIC <details style="margin:16px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC <summary style="padding:12px 16px;background:#F8F9FA;cursor:pointer;font-weight:600;font-size:14px;">📋 General Instructions（クリックで展開してコピー）</summary>
# MAGIC
# MAGIC <div style="padding:16px;">
# MAGIC <button onclick="copyInstructions()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="instructions-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC あなたはドラッグストアチェーンの店舗KPI分析アシスタントです。
# MAGIC
# MAGIC ## 基本ルール
# MAGIC 1. 日本語で回答してください
# MAGIC 2. 金額は円単位で、千の位にカンマを付けてください
# MAGIC 3. 前年比は % で表示し、上昇は ▲、下降は ▼ を付けてください
# MAGIC 4. 店舗比較時は全店平均も併記してください
# MAGIC
# MAGIC ## 用語の定義（Metric View に準拠）
# MAGIC - 「売上」「revenue」→ 売上合計（MEASURE: 売上合計）
# MAGIC - 「客単価」→ 売上合計 ÷ 客数合計（composable measure）
# MAGIC - 「調剤売上」→ カテゴリが「調剤」の売上（FILTER付きメジャー）
# MAGIC - 「物販売上」→ 調剤を除く売上（FILTER付きメジャー）
# MAGIC - 「食品構成比」→ 食品売上 ÷ 物販売上 × 100
# MAGIC
# MAGIC ## 分析のポイント
# MAGIC - 調剤併設の有無（has_pharmacy）で店舗特性が大きく異なる
# MAGIC - 食品構成比の推移はフード＆ドラッグ戦略の進捗を示す
# MAGIC - 坪効率（売上 ÷ 売場面積）は店舗評価の基本指標
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function copyInstructions() {
# MAGIC   var el = document.getElementById("instructions-block");
# MAGIC   if (!el) return;
# MAGIC   var text = el.innerText;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function() { alert("クリップボードにコピーしました"); }).catch(function() { fallbackCopyInst(text); });
# MAGIC   } else { fallbackCopyInst(text); }
# MAGIC }
# MAGIC function fallbackCopyInst(text) {
# MAGIC   var ta = document.createElement("textarea"); ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("クリップボードにコピーしました"); } catch(e) { alert("コピーできませんでした。手動でコピーしてください。"); }
# MAGIC   document.body.removeChild(ta);
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. サンプル質問を登録する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Genie スペースの「Sample questions」に以下を登録すると、ユーザーがワンクリックで試せます。
# MAGIC
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:16px 0;">
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;margin-bottom:6px;">SAMPLE 1 — 基本</div>
# MAGIC     <div style="font-size:14px;font-weight:600;color:#1B3139;">先月の全社売上と前年比を教えて</div>
# MAGIC   </div>
# MAGIC   <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-size:11px;color:#999;font-weight:600;margin-bottom:6px;">SAMPLE 2 — 比較</div>
# MAGIC     <div style="font-size:14px;font-weight:600;color:#1B3139;">調剤ありの店舗と無しで客単価を比較して</div>
# MAGIC   </div>
# MAGIC   <div style="background:#faf5ff;border:1px solid #AB47BC;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-size:11px;color:#AB47BC;font-weight:600;margin-bottom:6px;">SAMPLE 3 — エージェントモード</div>
# MAGIC     <div style="font-size:14px;font-weight:600;color:#1B3139;">最近調子が悪い店舗はどこ？原因も教えて</div>
# MAGIC   </div>
# MAGIC   <div style="background:#faf5ff;border:1px solid #AB47BC;border-radius:8px;padding:16px;">
# MAGIC     <div style="font-size:11px;color:#AB47BC;font-weight:600;margin-bottom:6px;">SAMPLE 4 — エージェントモード</div>
# MAGIC     <div style="font-size:14px;font-weight:600;color:#1B3139;">調剤をやってる店とやってない店、どっちが儲かってる？</div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 権限を設定する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:flex;gap:12px;align-items:flex-start;margin:12px 0;padding:16px 20px;background:#F8F9FA;border-radius:8px;">
# MAGIC   <div style="background:#FF3621;color:#fff;min-width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;">1</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:700;font-size:15px;">Genie スペースの右上「共有」をクリック</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="display:flex;gap:12px;align-items:flex-start;margin:12px 0;padding:16px 20px;background:#F8F9FA;border-radius:8px;">
# MAGIC   <div style="background:#FF3621;color:#fff;min-width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;">2</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:700;font-size:15px;">ユーザーまたはグループを追加</div>
# MAGIC     <div style="font-size:14px;color:#555;margin-top:4px;">デモ用: <code>all workspace users</code> を追加</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="display:flex;gap:12px;align-items:flex-start;margin:12px 0;padding:16px 20px;background:#F8F9FA;border-radius:8px;">
# MAGIC   <div style="background:#FF3621;color:#fff;min-width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;">3</div>
# MAGIC   <div>
# MAGIC     <div style="font-weight:700;font-size:15px;">権限レベルを選択</div>
# MAGIC     <div style="font-size:14px;color:#555;margin-top:4px;"><strong>Can query</strong>（閲覧・質問のみ）または <strong>Can edit</strong>（設定変更も可能）</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left:4px solid #ff9800;background:#fff3e0;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">⚠️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">注意: データアクセス権限</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Genie スペースの権限を付与しても、<strong>Unity Catalog のテーブル/Metric View へのアクセス権がないユーザーはクエリできません</strong>。<br/>
# MAGIC         対象ユーザーに対し、カタログ・スキーマの <code>USE</code> 権限と、テーブル/ビューの <code>SELECT</code> 権限が必要です。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. デモシナリオ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #ffc107;background:#fffde7;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">🎯</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">デモのポイント</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         「ダッシュボード（Step 4 で作成）と同じ Metric View を使っているから、<br/>
# MAGIC         <strong>Genie が返す数字は経営会議の帳票と完全に一致する</strong>」ことを強調してください。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lv.1 基本クエリ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:40%;">質問（自然言語）</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:30%;">Genie が使う MEASURE</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:30%;">デモポイント</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「先月の売上は？」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">MEASURE(売上合計)</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#e0f2f1;color:#00695c;padding:2px 8px;border-radius:4px;font-size:12px;">synonyms</span> 「売上」「revenue」どちらでもヒット</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「地域別に売上を見たい」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">MEASURE(売上合計) × dim 地域</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">同じ Metric View で<strong>切り口だけ変更</strong></td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「客単価が高い店舗トップ10は？」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">MEASURE(客単価) × dim 店舗名</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#e3f2fd;color:#1565c0;padding:2px 8px;border-radius:4px;font-size:12px;">composable</span> 定義済みの複合指標をそのまま使える</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lv.2 分析クエリ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:14px;">
# MAGIC   <thead>
# MAGIC     <tr style="background:#1B3139;color:#fff;">
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:40%;">質問（自然言語）</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:30%;">Genie が使う MEASURE</th>
# MAGIC       <th style="padding:10px 16px;text-align:left;font-weight:600;width:30%;">デモポイント</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「調剤ありとなしで売上を比較して」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">MEASURE(売上合計) × dim 調剤有無</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#fff3e0;color:#e65100;padding:2px 8px;border-radius:4px;font-size:12px;">FILTER</span> 付きメジャーの威力</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#F8F9FA;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「食品構成比が上がっている店舗は？」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">MEASURE(食品構成比) × dim 店舗名</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;"><span style="background:#e3f2fd;color:#1565c0;padding:2px 8px;border-radius:4px;font-size:12px;">composable</span> 複合指標もブレなく回答</td>
# MAGIC     </tr>
# MAGIC     <tr style="background:#fff;">
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-weight:600;">「前年比が悪い店舗の特徴は？」</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;font-family:monospace;font-size:12px;">複数 dim × measure の探索</td>
# MAGIC       <td style="padding:10px 16px;border-bottom:1px solid #E0E0E0;">Genie が自律的に切り口を探索</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lv.3 深掘りクエリ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left:4px solid #1976d2;background:#e3f2fd;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">ℹ️</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">エージェントモードとは？</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         複雑な質問に対して Genie が<strong>複数ステップの推論</strong>を行うモードです。<br/>
# MAGIC         「〇〇を分析して、原因を特定して、改善案を提案して」のような複合的な質問に対応できます。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC 以下の質問をそのまま入力してみてください：
# MAGIC
# MAGIC <div style="background:#1e1e1e;border-radius:8px;padding:16px 20px;margin:12px 0;overflow-x:auto;">
# MAGIC   <pre style="margin:0;color:#d4d4d4;font-family:'Fira Code','Consolas',monospace;font-size:13px;line-height:1.6;"><code>売上前年比が最も悪い店舗を特定し、
# MAGIC その店舗のカテゴリ別売上を全店平均と比較して、
# MAGIC どのカテゴリが特に弱いか教えてください。</code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#1e1e1e;border-radius:8px;padding:16px 20px;margin:12px 0;overflow-x:auto;">
# MAGIC   <pre style="margin:0;color:#d4d4d4;font-family:'Fira Code','Consolas',monospace;font-size:13px;line-height:1.6;"><code>調剤併設店の客単価と非併設店の客単価を月次で比較し、
# MAGIC その差が広がっているか縮まっているか、トレンドを教えてください。</code></pre>
# MAGIC </div>
# MAGIC
# MAGIC <div style="background:#1e1e1e;border-radius:8px;padding:16px 20px;margin:12px 0;overflow-x:auto;">
# MAGIC   <pre style="margin:0;color:#d4d4d4;font-family:'Fira Code','Consolas',monospace;font-size:13px;line-height:1.6;"><code>坪効率が全店平均を下回っている店舗のうち、
# MAGIC 食品構成比が高い店舗を抽出してください。
# MAGIC フード＆ドラッグ戦略が坪効率に与える影響を考察してください。</code></pre>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ダッシュボードとの一致を確認する

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0;">
# MAGIC   <div style="border:2px solid #7B61FF;border-radius:8px;padding:20px;background:#F3E5F5;">
# MAGIC     <div style="font-size:13px;font-weight:700;color:#7B61FF;margin-bottom:8px;">🤖 Genie に質問</div>
# MAGIC     <div style="font-size:14px;line-height:1.6;">
# MAGIC       <strong>「先月の全社売上は？」</strong><br/>
# MAGIC       → Genie が MEASURE(売上合計) から回答<br/>
# MAGIC       → 例: <strong style="font-size:18px;">12.3億円</strong>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="border:2px solid #1976d2;border-radius:8px;padding:20px;background:#E3F2FD;">
# MAGIC     <div style="font-size:13px;font-weight:700;color:#1976d2;margin-bottom:8px;">📊 ダッシュボードを確認</div>
# MAGIC     <div style="font-size:14px;line-height:1.6;">
# MAGIC       <strong>経営KPIレポート → 全社サマリ</strong><br/>
# MAGIC       → KPIカードの売上合計を確認<br/>
# MAGIC       → 例: <strong style="font-size:18px;">12.3億円</strong>
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
# MAGIC         <strong>同じ Metric View をデータソースにしているから、ダッシュボードと Genie の数字は完全に一致します。</strong><br/>
# MAGIC         現場のアドホック分析と経営帳票で数字がズレる問題は、もう起きません。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. チューニングのコツ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details style="margin:12px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC   <summary style="padding:12px 16px;background:#F8F9FA;cursor:pointer;font-weight:600;font-size:14px;">
# MAGIC     📖 Genie の回答精度を上げるには？
# MAGIC   </summary>
# MAGIC   <div style="padding:16px;font-size:14px;line-height:1.8;">
# MAGIC     <strong>1. Metric View の Agent Metadata を充実させる</strong><br/>
# MAGIC     <code>display_name</code>, <code>synonyms</code>, <code>format</code> を設定するほど、Genie の解釈精度が向上します。<br/><br/>
# MAGIC     <strong>2. テーブル・カラムコメントを追加する</strong><br/>
# MAGIC     <code>COMMENT ON TABLE</code> / <code>ALTER TABLE ... ALTER COLUMN ... COMMENT</code> で、データの意味を説明します。<br/><br/>
# MAGIC     <strong>3. Instructions に SQL Expression を追加する</strong><br/>
# MAGIC     テキスト指示よりも、具体的なSQL式を登録した方が Genie の精度が高くなります。<br/><br/>
# MAGIC     <strong>4. Monitoring タブでフィードバックを確認する</strong><br/>
# MAGIC     ユーザーが「👎」を付けた質問を確認し、Instructions を改善します。<br/><br/>
# MAGIC     <strong>5. データソースは少なく始める</strong><br/>
# MAGIC     最初は 5 テーブル/Metric View 以内で始め、精度を確認してから拡張します。
# MAGIC   </div>
# MAGIC </details>
# MAGIC
# MAGIC <details style="margin:12px 0;border:1px solid #E0E0E0;border-radius:8px;overflow:hidden;">
# MAGIC   <summary style="padding:12px 16px;background:#F8F9FA;cursor:pointer;font-weight:600;font-size:14px;">
# MAGIC     📖 Genie Space を埋め込むには？（Beta）
# MAGIC   </summary>
# MAGIC   <div style="padding:16px;font-size:14px;line-height:1.8;">
# MAGIC     Genie スペースは <strong>iframe で外部アプリケーションに埋め込み</strong>可能です（Beta）。<br/>
# MAGIC     社内ポータルや Databricks Apps に組み込むことで、ユーザーが専用画面に移動せずに質問できます。<br/><br/>
# MAGIC     詳細: <a href="https://docs.databricks.com/aws/en/genie/embed" target="_blank">Embed a Genie space | Databricks Docs</a>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <hr style="border:none;border-top:2px solid #E0E0E0;margin:32px 0;"/>
# MAGIC
# MAGIC <div style="border-left:4px solid #4caf50;background:#e8f5e9;border-radius:8px;padding:16px 20px;margin:16px 0;">
# MAGIC   <div style="display:flex;align-items:flex-start;gap:12px;">
# MAGIC     <span style="font-size:20px;">✅</span>
# MAGIC     <div>
# MAGIC       <div style="font-weight:700;font-size:15px;margin-bottom:4px;">Genie スペース作成完了</div>
# MAGIC       <div style="font-size:14px;color:#333;line-height:1.6;">
# MAGIC         Metric View をデータソースにした Genie スペースが完成しました。<br/>
# MAGIC         次のノートブックでは、同じ Metric View を使って経営 KPI ダッシュボードを作成します。
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | | |
# MAGIC |:--|--:|
# MAGIC | [← 05. Metric View 定義（応用）]($./05_Metric_View定義_応用) | [Next → 07. ダッシュボード作成]($./07_ダッシュボード作成) |
