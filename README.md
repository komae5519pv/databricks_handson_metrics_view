# UC Business Semantics ハンズオンデモ

ドラッグストアチェーンの店舗KPIを題材に、UC Business Semantics（Metric View）の価値と使い方を体験するデモコンテンツ。

## デモ概要

| 項目 | 内容 |
|------|------|
| 対象 | ドラッグストアチェーンのデータ活用推進担当 |
| 所要時間 | 約120分 |
| テーマ | 「同じKPIなのに数字が合わない」問題の解決 |
| 実行環境 | Databricks SQL Warehouse（サーバレス推奨） |
| 最低要件 | DBR 17.3+ / SQL Warehouse |

---

## デモストーリー（簡易）

```
1. 課題体験       → 同じKPIを3通りで書くと数字がズレる（Before）
2. Metric View    → Genie Code で YAML 定義を作成 & クエリ実践
3. 応用           → Composable / Window / semiadditive
4. Genie Space    → 現場が自然言語で分析、ダッシュボードと同じ数字
5. Dashboard      → Genie Code で経営KPIダッシュボードを構築、Genie 埋め込み
```

キーメッセージ: **「KPIの定義をデータ基盤で一元管理すれば、誰が・どのツールで分析しても、同じ数字になる」**

---

## ノートブック構成

| # | ファイル名 | 内容 | 実行環境 |
|---|-----------|------|---------|
| 00 | `00_config` | カタログ・スキーマ設定 | サーバレス / SQL WH |
| 01 | `01_サンプルデータ作成` | ドラッグストアデータ生成＋メタデータ・PK/FK・タグ付与 | サーバレス / SQL WH |
| 02 | `02_従来のやり方の課題` | 同じKPIの定義がバラける問題を体験 | SQL WH |
| 03 | `03_Metric_View定義_基本` | Genie Code で Metric View 作成 + FILTER付きメジャー | SQL WH |
| 04 | `04_Metric_Viewを使ってみる` | MEASURE()でクエリ実践 | SQL WH |
| 05 | `05_Metric_View定義_応用` | Composable / Window / semiadditive | SQL WH |
| 06 | `06_Genieスペース作成` | Genie Space 構築手順 + デモシナリオ | — (手順書) |
| 07 | `07_ダッシュボード作成` | Genie Code で経営KPIダッシュボード作成 + Genie 埋め込み | — (手順書) |

---

## ノートブック詳細

### 00_config
- 変数設定（`catalog_name`, `schema_name`）
- カタログ・スキーマ作成
- リセット用コード（コメントアウト）
- **全ノートブックで `%run ./00_config` により変数を共有**

### 01_サンプルデータ作成
- 7テーブルを生成（下記データモデル参照）
- テーブルコメント・カラムコメント付与
- PK/FK制約設定（NOT NULL + PRIMARY KEY / FOREIGN KEY）
- タグ付与（source, grain, pii 等）
- S046〜S050 は2025年新規オープン店舗（前年比デモ用）

### 02_従来のやり方の課題
- 「客単価」を3通りのSQLで計算 → 結果が異なる（比較カード付き）
- 「調剤売上込み vs 物販のみ」で食品構成比が変わる
- 「既存店前年比」の定義が1年/2年で結果が違う
- 実務での定義ブレ例（改装/業態変更/移転/閉店予定）の補足あり

### 03_Metric_View定義_基本
- YAML構造の読み方テーブル（version/source/filter/dimensions/measures/joins）
- 各プロパティの解説（name/expr/display_name/synonyms/format/comment）
- **メイン: Catalog Explorer + Genie Code で作成**（日本語プロンプト付き）
- オプション: SQL（`spark.sql(f"...")` で変数化済み）での作成
- 正解YAML（折りたたみ + コピーボタン）
- Mermaid アーキテクチャ図
- 作成する Metric View:
  - `drugstore_kpi_metrics` — 日別売上の基本KPI
  - `drugstore_store_metrics` — カテゴリ別×店舗のKPI（JOIN + FILTER付きメジャー）

### 04_Metric_Viewを使ってみる
- MEASURE()の基本クエリ（月別・曜日別）
- カテゴリ×店舗の Metric View（地域別・調剤有無）
- Metric View なしの場合の生SQL比較（折りたたみ、地域別クエリで20行→7行の対比）
- Before/After まとめカード

### 05_Metric_View定義_応用
- Composable Measures（客単価・坪効率）— GA
- Window Measures（7日移動平均・累計売上）— Experimental
- Materialization の概念紹介 — Experimental

### 06_Genieスペース作成
- Genie Space 作成手順（タイムライン形式）
- データソース: Metric View + stores テーブル
- Instructions（コピーボタン付き折りたたみ）
- サンプル質問（基本2 + エージェントモード2）
- 権限設定手順
- デモシナリオ（Lv.1 基本 / Lv.2 分析 / Lv.3 深掘り）
- ダッシュボードとの数字一致確認
- チューニングのコツ（折りたたみ）

### 07_ダッシュボード作成
- Genie Code でダッシュボードを作成する手順
- ページ1 全社サマリ（KPIスコアカード前月比較 + 月次推移予測 + カテゴリ別 + 地域別）
- ページ2 店舗パフォーマンス（店舗一覧 + 調剤比較 + タイプ別）
- ページ3 調剤・カテゴリ分析（カテゴリ推移 + 調剤vs物販 + 食品構成比）
- ダッシュボード調整Tips（レイアウト/チャート変更/フィルタ追加）
- Genie スペースのダッシュボード埋め込み手順
- Genie スペースとの数字一致確認
- ハンズオン全体の振り返り

---

## データモデル

### テーブル一覧

| テーブル名 | 説明 | 件数目安 |
|-----------|------|---------|
| `stores` | 店舗マスタ | 50店舗（うち5店舗は2025年新規オープン） |
| `categories` | カテゴリマスタ | 7カテゴリ |
| `sales_daily` | 日別売上 | ~91,000件（2021-2025年、5年分） |
| `sales_by_category` | カテゴリ別月次売上 | ~21,000件（2021-2025年、5年分） |
| `prescription_monthly` | 調剤月次実績 | ~1,800件（調剤併設店のみ、2021-2025年） |
| `members` | 会員マスタ | 100,000人 |
| `member_visits` | 会員来店月次サマリ | ~300,000件 |

### カテゴリ構成

| ID | カテゴリ | 売上構成比 | 粗利率 |
|----|---------|-----------|--------|
| CAT01 | 医薬品（OTC） | 15% | 40% |
| CAT02 | 調剤 | 12% | 35% |
| CAT03 | 化粧品 | 18% | 30% |
| CAT04 | 日用品 | 20% | 20% |
| CAT05 | 食品・飲料 | 25% | 15% |
| CAT06 | 健康食品・サプリ | 5% | 45% |
| CAT07 | ベビー・介護用品 | 5% | 25% |

### stores テーブル カラム

| カラム | 型 | 説明 |
|--------|------|------|
| store_id | STRING | 店舗ID（PK）S001〜S050 |
| store_name | STRING | 店舗名 |
| prefecture | STRING | 都道府県 |
| city | STRING | 市区町村 |
| region | STRING | 地域（北海道/東北/関東/中部/東海/関西/中国/四国/九州） |
| latitude | DOUBLE | 緯度 |
| longitude | DOUBLE | 経度 |
| size_sqm | INT | 売場面積（平方メートル） |
| open_date | DATE | 開店日 |
| store_type | STRING | 店舗タイプ（駅前型/郊外型/ロードサイド型） |
| has_pharmacy | BOOLEAN | 調剤併設有無 |
| parking_capacity | INT | 駐車場台数 |

### sales_daily テーブル カラム

| カラム | 型 | 説明 |
|--------|------|------|
| store_id | STRING | 店舗ID（FK） |
| sales_date | DATE | 売上日（2021-01-01〜2025-12-31） |
| sales_amount | INT | 売上金額（円） |
| customer_count | INT | 客数 |
| receipt_count | INT | レシート枚数 |

---

## 技術スタック

| レイヤー | 技術 | 用途 |
|---------|------|------|
| データ基盤 | Unity Catalog | カタログ・ガバナンス |
| テーブル形式 | Delta Lake | データストレージ |
| KPI定義 | UC Business Semantics（Metric View） | 指標の一元管理 |
| KPI定義言語 | YAML v1.1 | Metric View定義 |
| クエリ | MEASURE() 関数 | Metric Viewのクエリ |
| AI支援 | Genie Code | Metric View・ダッシュボード自動生成 |
| 自然言語分析 | Genie Space | エンドユーザー向け分析 |
| 可視化 | AI/BI Dashboard | 経営帳票 |
| 実行環境 | SQL Warehouse（サーバレス） | 全ノートブック共通 |

### Metric View 機能カバレッジ

| 機能 | ノートブック | ステータス |
|------|-----------|-----------|
| Dimensions - カラムそのまま | 03 | GA |
| Dimensions - CASE WHEN変換 | 03 | GA |
| Dimensions - 関数変換（DATE_TRUNC等） | 03 | GA |
| Measures - 基本集計（SUM/COUNT/AVG） | 03 | GA |
| Measures - FILTER付き | 03 | GA |
| Measures - Composable（MEASURE参照） | 05 | GA |
| Measures - Window trailing | 05 | Experimental |
| Measures - Window cumulative | 05 | Experimental |
| Measures - semiadditive | 05 | Experimental |
| Agent Metadata（display_name, synonyms, format, comment） | 03 | GA |
| Star schema JOIN | 03, 05 | GA |
| Materialization | 05（紹介のみ） | Experimental |

---

## Metric View 定義一覧

### drugstore_kpi_metrics（日別売上の基本KPI）

| 種類 | 名前 | 説明 |
|------|------|------|
| Source | sales_daily | 日別売上テーブル |
| Dimension | 売上日 / 売上月 / 売上年 / 曜日 | 日付の切り口 |
| Measure | 売上合計 | SUM(sales_amount) |
| Measure | 客数合計 | SUM(customer_count) |
| Measure | レシート枚数合計 | SUM(receipt_count) |
| Measure | 客単価 | 売上合計 / 客数合計（Composable） |
| Measure | 客単価レシートベース | 売上合計 / レシート枚数合計（Composable） |

### drugstore_store_metrics（カテゴリ別×店舗のKPI）

| 種類 | 名前 | 説明 |
|------|------|------|
| Source | sales_by_category | カテゴリ別月次売上 |
| JOIN | stores, categories | 店舗マスタ・カテゴリマスタを結合 |
| Dimension | 売上月 / 店舗名 / 地域 / 店舗タイプ / 調剤の有無 / カテゴリ名 | |
| Measure | カテゴリ別売上 | SUM(sales_amount) |
| Measure | 調剤売上 | FILTER(カテゴリ = '調剤') |
| Measure | 物販売上 | FILTER(カテゴリ != '調剤') |
| Measure | 食品売上 | FILTER(カテゴリ = '食品・飲料') |
| Measure | 食品構成比 | 食品売上 / 物販売上（percentage） |

### drugstore_advanced_metrics（応用：Composable + Window）

| 種類 | 名前 | 説明 |
|------|------|------|
| Source | sales_daily | 日別売上 |
| JOIN | stores | 店舗マスタ結合 |
| Measure | 客単価 | 売上合計 / 客数合計（Composable） |
| Measure | 坪効率 | 売上合計 / 売場面積 × 3.3（Composable） |
| Measure | 7日移動平均売上 | trailing 7 day window |
| Measure | 累計売上 | cumulative window |

---

## ダッシュボード帳票設計

### ダッシュボード名: 店舗経営KPIレポート

#### ページ1: 全社サマリ

| ウィジェット | チャート種類 | データソース |
|------------|-----------|------------|
| KPIスコアカード ×4 | Counter（前月比較付き） | 売上合計 / 客数合計 / 客単価 / 食品構成比 |
| 月次売上推移 | 折れ線グラフ | 売上合計 × 売上月 |
| カテゴリ構成比 | 円グラフ or 棒グラフ | カテゴリ別売上 × カテゴリ名 |
| 地域別売上ランキング | 横棒グラフ | 売上合計 × 地域 |

#### ページ2: 店舗パフォーマンス

| ウィジェット | チャート種類 | データソース |
|------------|-----------|------------|
| 店舗別KPI一覧 | テーブル | 売上/客数/客単価/食品構成比 |
| 調剤併設 vs 非併設 | 棒グラフ | 売上合計 × 調剤の有無 |
| 店舗タイプ別売上 | 棒グラフ | 売上合計 × 店舗タイプ |

#### ページ3: 調剤・カテゴリ分析

| ウィジェット | チャート種類 | データソース |
|------------|-----------|------------|
| カテゴリ別月次推移 | 折れ線グラフ | カテゴリ別売上 × カテゴリ名 × 売上月 |
| 調剤売上 vs 物販売上 | 折れ線グラフ | 調剤売上 / 物販売上 × 売上月 |
| 食品構成比推移 | 折れ線グラフ | 食品構成比 × 売上月 |

---

## Genie Space 設定

| 設定項目 | 内容 |
|---------|------|
| Space名 | 店舗KPI分析アシスタント |
| データソース | drugstore_kpi_metrics, drugstore_store_metrics, stores |
| Warehouse | SQL Warehouse（サーバレス推奨） |

### サンプル質問

| # | 種類 | 質問 |
|---|------|------|
| 1 | 基本 | 先月の全社売上と前年比を教えて |
| 2 | 比較 | 調剤ありの店舗と無しで客単価を比較して |
| 3 | エージェント | 最近調子が悪い店舗はどこ？原因も教えて |
| 4 | エージェント | 調剤をやってる店とやってない店、どっちが儲かってる？ |

---

## 汎用性について

- カタログ名・スキーマ名は `00_config` で変数設定。全ノートブックで `%run ./00_config` により共有
- SQL セルの Metric View 作成は `spark.sql(f"...")` で変数化済み
- ノートブック間リンクは `$./ノートブック名` の相対パス（Git clone 対応）
- コピー用YAMLには「カタログ名・スキーマ名はご自身の環境に合わせて読み替えてください」の注記あり

---

## 参考リンク

- [UC Business Semantics 公式ドキュメント](https://docs.databricks.com/aws/en/business-semantics/)
- [Metric Views 作成](https://docs.databricks.com/aws/en/business-semantics/metric-views/create-edit)
- [YAML リファレンス](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference)
- [Curate an effective Genie space](https://docs.databricks.com/aws/en/genie/best-practices)
- [GA & OSS 発表ブログ](https://www.databricks.com/blog/redefining-semantics-data-layer-future-bi-and-ai)
