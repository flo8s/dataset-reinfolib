## データ出典

[国土交通省 不動産情報ライブラリ](https://www.reinfolib.mlit.go.jp/)の API から取得した不動産取引価格情報です。
2005年第3四半期から現在までの全国の取引データを収録しています。

## テーブル: mart_trade_prices

主なカラム:

- property_type: 種類（宅地、土地、中古マンション等、農地、林地）
- prefecture / municipality / district_name: 都道府県 / 市区町村 / 地区名
- trade_price: 取引価格（総額・円）
- unit_price: 平米単価
- price_per_unit: 坪単価
- area: 面積（平米）
- floor_plan: 間取り
- building_year: 建築年
- structure: 建物の構造
- city_planning: 都市計画
- coverage_ratio / floor_area_ratio: 建ぺい率 / 容積率（%）
- year / quarter: 取得年 / 四半期
- price_category: 価格区分（取引価格 / 成約価格）

## クレジット

このサービスは、国土交通省の不動産情報ライブラリのAPI機能を使用していますが、提供情報の最新性、正確性、完全性等が保証されたものではありません。

## ライセンス

[政府標準利用規約 第2.0版](https://www.kantei.go.jp/jp/singi/it2/densi/kettei/gl2_betten_1.pdf)
