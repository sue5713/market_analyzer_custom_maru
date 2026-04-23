# バックアップ運用ガイド

## 概要
`backup/` 配下に、変更前の重要ファイルのスナップショットを退避します。新しい実装で不具合が出たらここから簡単に元に戻せるようにするためです。

## 命名規則
`backup/<YYYYMMDD>_<変更の概要>/`

## 退避対象
その変更で書き換えるファイルのみを格納します。

---

## 既存バックアップ一覧

### `backup/20260424_pre_hours_filter/` (2026-04-24)
**変更内容**: weekday 基点 wrapper と US 15:45 NY 終値フィルタ + close-to-close 合成バー方式を導入 (本リポには以前 wrapper が存在しなかったため、退避対象なしで新規導入)
- Before: `analyze_sectors.py` を直接走らせ、直近14日を対象に RF/MDD を算出。yfinance が返した最終バー(after-hours が混ざるケースあり)をそのまま終値として使用
- After:
  - `run_with_baseline.py` (wrapper) で曜日判定 -> 適切な `--start/--end` を決める
    - **月〜木**: 前週末(前週金曜)の終値を基点
    - **金**: 前月末の終値を基点
  - NY 時間 9:30 〜 15:45 のバーのみにフィルタ
  - JST で見ると、**夏時間(EDT)** は 04:45 開始の 15分足 / **冬時間(EST)** は 05:45 開始の 15分足 が最終バー
  - DST/EST の切替は pytz が自動判定
  - **close-to-close 合成バー**: baseline 日の最終バー Close を OHLC 全部に入れた合成バーを先頭に差し込み、baseline 日の実バーを除外。下流の analyze_sectors は Open→Close の計算式を変えずに終値-終値基準で RF/MDD を算出

**実装方針**: `analyze_sectors.py` は未変更。新規 `market_hours_filter.py` / `run_with_baseline.py` を追加し、`run_with_baseline.py` で `analyze_sectors.filter_data_by_date` を monkey-patch して同一プロセス内で main を呼び出す構成。

**退避ファイル**: なし (以前の wrapper が存在しなかったため)

---

## 復元手順

### A. バックアップファイルから戻す (簡単)
次回以降の変更から利用。今回の 20260424 導入に関しては退避ファイルがないため、完全に元に戻すには B の commit revert を使ってください。

### B. Git で変更コミットごと revert (バイト完全一致)
```bash
git log --oneline
git revert <commit-hash>
git push
```

---

## ルール
- `backup/` 配下のファイルは **編集禁止**
- 大きな変更の前には **必ず新しい `backup/<timestamp>_<label>/`** を切り対象ファイルをコピー
- `backup/` は GitHub Actions 実行時に読み込まれません
