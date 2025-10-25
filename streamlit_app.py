import streamlit as st
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import pandas as pd
import io
import pydeck as pdk # pydeck をインポート

KISHOU_XML_PAGE_URL = "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"

st.set_page_config(page_title="気象庁 防災情報XML（長期フィード）「気象特別警報・警報・注意報」発表履歴検索ツール", layout="wide")

# --- 説明セクション（気象庁防災情報XMLの概要） ---
with st.expander("📘 気象庁防災情報XMLとは？", expanded=True):
    st.markdown("""
    ## 防災情報XMLとは
    - 気象庁は、気象・津波・地震・火山などの防災情報を迅速かつ正確に伝えるために
    「気象庁防災情報XMLフォーマット」を策定・公開しています。
    - 防災情報XMLは .XML 形式で機械可読な情報が提供され、報道機関・自治体・防災アプリ等での自動処理・配信に活用されています。一般人の私たちでもインターネットからPull型で自由に取得可能です。
    本サイトでは streamlit community cloud の練習用として、Atom 随時フィールドから「気象特別警報・警報・注意報」を取得して .csv(BOM付)で出力できるようにしました。

    - 参考: https://xml.kishou.go.jp/
    
    """)

@st.cache_data(ttl=600)
def fetch_feed(url: str, hours_threshold: int = 48):
    """
    指定されたURLからAtomフィードを取得し、リンクされているXMLデータを（指定時間内のエントリーのみ）取得します。
    """
    fetched = {"main_feed_xml": None, "linked_entries_xml": []}
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        fetched["main_feed_xml"] = resp.content

        root = ET.fromstring(fetched["main_feed_xml"].decode("utf-8"))
        atom_ns = "{http://www.w3.org/2005/Atom}"

        for entry in root.findall(f"{atom_ns}entry"):
            entry_info = {
                "EntryID": entry.find(f"{atom_ns}id").text if entry.find(f"{atom_ns}id") is not None else "N/A",
                "FeedReportDateTime": entry.find(f"{atom_ns}updated").text if entry.find(f"{atom_ns}updated") is not None else "N/A",
                "FeedTitle": entry.find(f"{atom_ns}title").text if entry.find(f"{atom_ns}title") is not None else "N/A",
                "Author": entry.find(f"{atom_ns}author/{atom_ns}name").text if entry.find(f"{atom_ns}author/{atom_ns}name") is not None else "N/A",
                "LinkedXMLData": None,
                "LinkedXMLUrl": None
            }

            feed_report_time_str = entry_info.get("FeedReportDateTime")
            skip_by_time = False
            if feed_report_time_str and feed_report_time_str != "N/A":
                try:
                    # タイムゾーン情報を正しく処理
                    if feed_report_time_str.endswith("Z"):
                        feed_report_time = datetime.fromisoformat(feed_report_time_str[:-1]).replace(tzinfo=timezone.utc)
                    else:
                        feed_report_time = datetime.fromisoformat(feed_report_time_str)
                    
                    # 取得した時間がしきい値より古い場合はスキップ
                    if feed_report_time < time_threshold:
                        skip_by_time = True
                except Exception:
                    pass # 時刻のパースに失敗した場合はスキップしない（次の処理で試行）

            linked_xml_link_element = entry.find(f'{atom_ns}link[@type="application/xml"]')
            # 時間でスキップフラグが立っておらず、リンク要素が存在する場合のみXMLを取得
            if linked_xml_link_element is not None and not skip_by_time:
                linked_xml_url = linked_xml_link_element.get("href")
                if linked_xml_url:
                    try:
                        lx_resp = requests.get(linked_xml_url, timeout=15)
                        lx_resp.raise_for_status()
                        entry_info["LinkedXMLData"] = lx_resp.content
                        entry_info["LinkedXMLUrl"] = linked_xml_url
                    except Exception as e:
                        entry_info["LinkedXMLData"] = None
                        entry_info["LinkedXMLError"] = str(e)
            fetched["linked_entries_xml"].append(entry_info)

    except Exception as e:
        fetched["error"] = str(e)

    return fetched

def parse_warnings_advisories(fetched_data, hours_threshold: int = 48):
    """
    fetch_feedから取得したデータのうち、「気象特別警報・警報・注意報」のみをパースします。
    """
    parsed = []
    if not fetched_data or not fetched_data.get("linked_entries_xml"):
        return parsed

    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

    for entry in fetched_data["linked_entries_xml"]:
        feed_title = entry.get("FeedTitle", "N/A")
        # 「気象特別警報・警報・注意報」以外はスキップ
        if feed_title != "気象特別警報・警報・注意報":
            continue

        # fetch_feedで時間フィルタリングしているが、念のためここでも確認
        feed_time_str = entry.get("FeedReportDateTime")
        try:
            if feed_time_str and feed_time_str.endswith("Z"):
                feed_time = datetime.fromisoformat(feed_time_str[:-1]).replace(tzinfo=timezone.utc)
            elif feed_time_str:
                feed_time = datetime.fromisoformat(feed_time_str)
            else:
                feed_time = None
        except Exception:
            feed_time = None

        if feed_time and feed_time < time_threshold:
            continue

        extracted = {
            "EntryID": entry.get("EntryID", "N/A"),
            "FeedReportDateTime": entry.get("FeedReportDateTime", "N/A"),
            "FeedTitle": feed_title,
            "Author": entry.get("Author", "N/A"),
            "LinkedXMLDataPresent": bool(entry.get("LinkedXMLData")),
            "LinkedXMLUrl": entry.get("LinkedXMLUrl", "")
        }

        linked_bytes = entry.get("LinkedXMLData")
        warnings = []
        report_dt = extracted["FeedReportDateTime"] # デフォルト値

        if linked_bytes:
            try:
                xml_text = linked_bytes.decode("utf-8")
            except Exception:
                xml_text = linked_bytes.decode("utf-8", errors="replace") # デコードエラー時は置換
            try:
                root = ET.fromstring(xml_text)
                # XML内のReportDateTimeを取得
                rt = root.find('.//{*}ReportDateTime')
                if rt is not None and rt.text:
                    report_dt = rt.text

                # ヘッドライン（概要）を取得
                headline = root.find('.//{*}Headline/{*}Text')
                overall_detail = headline.text if headline is not None and headline.text else "N/A"

                # 各警報・注意報アイテムをパース
                items = root.findall('.//{*}Item')
                for item in items:
                    kind_el = item.find('.//{*}Kind/{*}Name')
                    area_el = item.find('.//{*}Areas/{*}Area/{*}Name')
                    # Area/Name がない場合、Prefecture/Name を試す
                    if area_el is None:
                        area_el = item.find('.//{*}Areas/{*}Area/{*}Prefecture/{*}Name')

                    kind = kind_el.text if kind_el is not None and kind_el.text else "N/A"
                    area = area_el.text if area_el is not None and area_el.text else "N/A"

                    if kind != "N/A" or area != "N/A":
                        warnings.append({"Kind": kind, "Area": area, "Detail": overall_detail})
            except ET.ParseError:
                warnings.append({"Kind": "解析エラー", "Area": "解析エラー", "Detail": "XML解析エラー"})
            except Exception:
                warnings.append({"Kind": "エラー", "Area": "エラー", "Detail": "不明なエラー"})
        else:
            # リンクされたXMLデータがない場合 (fetch_feedでスキップされた場合など)
            if entry.get("LinkedXMLError"):
                 warnings.append({"Kind": "取得エラー", "Area": "取得エラー", "Detail": entry.get("LinkedXMLError")})
            elif not extracted["LinkedXMLDataPresent"]:
                 warnings.append({"Kind": "データなし", "Area": "データなし", "Detail": "時間外または取得対象外"})
            else:
                 warnings.append({"Kind": "取得失敗", "Area": "取得失敗", "Detail": "リンクXMLがありません"})


        # パースした警報情報があり、かつ元データが存在した場合のみリストに追加
        if warnings and extracted["LinkedXMLDataPresent"]:
            extracted["ReportDateTime"] = report_dt # XML内の日時で更新
            extracted["WarningsAdvisories"] = warnings
            parsed.append(extracted)

    return parsed

# --- Streamlit UI ---

st.title("気象庁 防災情報 (XML) ビューア")

col1, col2 = st.columns([1, 2])
with col1:
    st.markdown("### 設定")
    hours = st.number_input("何時間以内のフィードを取得しますか？", min_value=1, max_value=168, value=48, step=1)
    if st.button("フィード取得 / 更新"):
        # キャッシュをクリアして再実行
        st.cache_data.clear()
        st.rerun()

with col2:
    st.markdown("### フィード取得状況")
    with st.spinner("フィードを取得しています..."):
        data = fetch_feed(KISHOU_XML_PAGE_URL, hours_threshold=hours)

if data.get("error"):
    st.error(f"取得中にエラーが発生しました: {data['error']}")

entries = data.get("linked_entries_xml", [])
st.markdown(f"**フィード内エントリー数**: {len(entries)} （うち、{hours}時間以内のXML取得対象エントリーのみ処理）")

# Atom フィードの CSV ダウンロード機能
if entries:
    atom_feed_df = pd.DataFrame(entries)
    # LinkedXMLData列は重いのでCSVからは削除
    if "LinkedXMLData" in atom_feed_df.columns:
        atom_feed_df = atom_feed_df.drop(columns=["LinkedXMLData"])
        
    csv_buffer_atom = io.StringIO()
    atom_feed_df.to_csv(csv_buffer_atom, index=False, encoding="utf-8-sig")
    st.download_button(
        label="Atom フィードを CSV でダウンロード",
        data=csv_buffer_atom.getvalue().encode("utf-8-sig"),  # BOM付きUTF-8
        file_name=f"atom_feed_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
        mime="text/csv"
    )

# --- 警報・注意報データの処理 ---
parsed = parse_warnings_advisories(data, hours_threshold=hours)

if parsed:
    transformed_data_for_db = []
    count_placeholder = st.empty()  # カウントアップ用のプレースホルダー
    count = 0
    for p in parsed:
        for wa in p.get("WarningsAdvisories", []):
            transformed_data_for_db.append({
                "ReportDateTime": p.get("ReportDateTime"),
                "Title": p.get("FeedTitle"),
                "Author": p.get("Author"),
                "Kind": wa.get("Kind"),
                "Area": wa.get("Area"),
                "Detail": wa.get("Detail"),
                "EntryID": p.get("EntryID")
            })
            count += 1
            count_placeholder.info(f"{count} 件の警報・注意報データを読み込み中...")  # 同じ枠内で更新

    csv_buffer_warnings = io.StringIO()
    df = pd.DataFrame(transformed_data_for_db)
    df.to_csv(csv_buffer_warnings, index=False, encoding="utf-8-sig")
    count_placeholder.success(f"{count} 件の警報・注意報データの読み込みが完了しました！")  # 完了メッセージ
    
    st.download_button(
        label="警報・注意報データ（生）を CSV でダウンロード",
        data=csv_buffer_warnings.getvalue().encode("utf-8-sig"),  # BOM付きUTF-8
        file_name=f"warnings_raw_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
        mime="text/csv"
    )

    # --- ▼▼▼【pydeck】のための緯度・経度マッピング ▼▼▼ ---
    st.markdown("---")
    st.markdown("### 🗺️ 警報・注意報 発令地域マップ (Pydeck)")

    # 緯度・経度マッピング辞書（簡易版：主要都道府県・地域）
    AREA_LAT_LON_MAP = {
        "北海道": [43.06, 141.35], "札幌": [43.06, 141.35], "青森県": [40.82, 140.74],
        "岩手県": [39.70, 141.15], "宮城県": [38.27, 140.87], "仙台": [38.27, 140.87],
        "秋田県": [39.72, 140.10], "山形県": [38.24, 140.36], "福島県": [37.75, 140.47],
        "茨城県": [36.34, 140.45], "栃木県": [36.57, 139.88], "群馬県": [36.39, 139.06],
        "埼玉県": [35.86, 139.65], "千葉県": [35.61, 140.12], "東京都": [35.69, 139.69],
        "千代田区": [35.69, 139.75], "伊豆諸島北部": [34.74, 139.40], "伊豆諸島南部": [33.11, 139.79],
        "小笠原諸島": [26.65, 142.20], "神奈川県": [35.45, 139.64], "横浜": [35.45, 139.64],
        "新潟県": [37.90, 139.02], "富山県": [36.70, 137.21], "石川県": [36.59, 136.63],
        "福井県": [36.07, 136.22], "山梨県": [35.66, 138.57], "長野県": [36.65, 138.18],
        "岐阜県": [35.42, 136.72], "静岡県": [34.98, 138.38], "愛知県": [35.18, 136.91],
        "名古屋": [35.18, 136.91], "三重県": [34.73, 136.51], "滋賀県": [35.00, 135.87],
        "京都府": [35.02, 135.76], "大阪府": [34.69, 135.50], "兵庫県": [34.69, 135.18],
        "奈良県": [34.69, 135.83], "和歌山県": [34.23, 135.17], "鳥取県": [35.50, 134.24],
        "島根県": [35.47, 133.05], "岡山県": [34.66, 133.93], "広島県": [34.40, 132.46],
        "山口県": [34.19, 131.47], "徳島県": [34.07, 134.56], "香川県": [34.34, 134.04],
        "愛媛県": [33.84, 132.77], "高知県": [33.56, 133.53], "福岡県": [33.61, 130.40],
        "佐賀県": [33.26, 130.30], "長崎県": [32.75, 129.88], "熊本県": [32.80, 130.71],
        "大分県": [33.24, 131.61], "宮崎県": [31.91, 131.42], "鹿児島県": [31.56, 130.56],
        "沖縄県": [26.21, 127.68], "沖縄本島地方": [26.21, 127.68], "宮古島地方": [24.80, 125.28],
        "八重山地方": [24.34, 124.16]
    }

    if 'df' in locals() and df is not None and not df.empty:
        with st.spinner("地図データを準備しています..."):
            df_map = df.copy()
            
            # 'Area' 列をマップのキーと照合
            df_map['lat'] = df_map['Area'].map(lambda x: AREA_LAT_LON_MAP.get(x, [None, None])[0])
            df_map['lon'] = df_map['Area'].map(lambda x: AREA_LAT_LON_MAP.get(x, [None, None])[1])
            
            # 緯度・経度が見つからなかったデータを削除
            df_map.dropna(subset=['lat', 'lon'], inplace=True)

            if not df_map.empty:
                # 警報・注意報の種類（Kind）で色分けする
                def get_color(kind_str):
                    if "特別警報" in kind_str:
                        return [255, 0, 255, 180] # 紫 (不透明度追加)
                    elif "警報" in kind_str:
                        return [255, 0, 0, 180] # 赤
                    elif "注意報" in kind_str:
                        return [255, 255, 0, 180] # 黄
                    else:
                        return [128, 128, 128, 180] # グレー
                
                df_map['color'] = df_map['Kind'].apply(get_color)
                
                # ツールチップ用のテキストを作成 (Detailが長い場合に備えてラップ)
                df_map['tooltip'] = df_map.apply(lambda row: f"{row['Area']}: {row['Kind']}\n{''.join([s + ('\n' if (i + 1) % 40 == 0 else '') for i, s in enumerate(str(row['Detail']))])}", axis=1)


                # Pydeckの設定
                view_state = pdk.ViewState(
                    latitude=35.69, # 日本の中心あたり
                    longitude=139.69,
                    zoom=4,
                    pitch=50,
                )

                layer = pdk.Layer(
                    'ScatterplotLayer',
                    df_map,
                    get_position=['lon', 'lat'],
                    get_color='color',
                    get_radius=15000, # 15km
                    pickable=True,
                    auto_highlight=True,
                )
                
                tooltip_html = {
                    "html": "<b>情報:</b><br><div style='white-space: pre-wrap; max-width: 300px; font-size: 12px;'>{tooltip}</div>",
                    "style": {
                        "backgroundColor": "steelblue",
                        "color": "white",
                    }
                }

                st.pydeck_chart(pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    map_style='mapbox://styles/mapbox/light-v9',
                    tooltip=tooltip_html,
                ))
                
                with st.expander("マップデータの詳細（緯度・経度が付与されたデータ）"):
                    st.dataframe(df_map[['Area', 'Kind', 'lat', 'lon', 'Detail']])

            else:
                st.warning("地図にプロットできる地域データ（緯度・経度マップと一致）がありませんでした。")
                st.info(f"（検出された主な地域名: {list(df['Area'].unique())}）")

    else:
        st.info("地図表示対象のデータがありません。")

    # --- ▲▲▲【pydeck】のロジックをここまで追加 ▲▲▲ ---


    # --- ▼▼▼【改修されたコード2】のロジックをここから追加 ▼▼▼ ---
    st.markdown("---") # 区切り線
    st.markdown("### 📊 警報・注意報 ピボットテーブル (地域別)")

    # dfが利用可能で空でないか確認します。
    if 'df' in locals() and df is not None and not df.empty:
        with st.spinner("ピボットテーブルを作成しています..."):
            try:
                # ReportDateTimeをdatetimeオブジェクトに変換し、日付のみを保持します。
                df_pivot = df.copy() # 元のdfを変更しないようにコピー
                df_pivot['ReportDateTime_cleaned'] = pd.to_datetime(df_pivot['ReportDateTime'], errors='coerce')
                df_pivot['ReportDateTime_cleaned'] = df_pivot['ReportDateTime_cleaned'].dt.tz_convert(None) # タイムゾーン削除
                df_pivot['ReportDate'] = df_pivot['ReportDateTime_cleaned'].dt.date # 日付のみ取得
            except Exception as e:
                st.error(f"ReportDateTimeの変換エラー: {e}")
                # 変換に失敗した場合、エラーを表示し、ReportDate列はNaTで埋める
                df_pivot['ReportDate'] = pd.NaT 

            # ReportDateが正常に作成されたか確認 (すべてNaTでないか)
            if 'ReportDate' in df_pivot.columns and not df_pivot['ReportDate'].isnull().all():
                try:
                    # 各警報・注意報の種類（Kind）とReportDate, Title, Authorの組み合わせごとに、対象地域（Area）のリストを集計します。
                    area_kind_summary_df = df_pivot.groupby(['ReportDate', 'Title', 'Author', 'Kind'])['Area'].agg(lambda x: list(x.unique())).reset_index()
                    area_kind_summary_df = area_kind_summary_df.rename(columns={'Area': '対象地域リスト'})

                    # (オプション) 中間データを折りたたみで表示
                    with st.expander("中間データ: 種類ごとの対象地域リスト（ユニーク）"):
                        st.dataframe(area_kind_summary_df)

                    # '対象地域リスト' 列を展開して、リストの各要素が新しい行になるようにします。
                    expanded_df = area_kind_summary_df.explode('対象地域リスト')

                    # ピボットテーブルを作成します。
                    pivot_by_area_kind = pd.pivot_table(
                        expanded_df.fillna({'Kind': '不明な種類', '対象地域リスト': '不明な地域'}),
                        values='Kind', # 集計対象はKind自体（存在すれば1）
                        index=['ReportDate', 'Title', 'Author', '対象地域リスト'], # 対象地域をインデックスに含める
                        columns='Kind',
                        aggfunc='size', # 各組み合わせの出現回数をカウント（存在すれば1）
                        fill_value=0 # 欠損値を0で埋めます（発令なしを示す）
                    )

                    st.success("各地域ごとの警報/注意報の発令状況（ピボットテーブル）が正常に作成されました。")
                    st.dataframe(pivot_by_area_kind) # StreamlitでDataFrameを表示

                    # ピボットテーブルをCSVファイルに保存（ダウンロードボタン）
                    csv_buffer_pivot = io.StringIO()
                    # MultiIndexを維持したままCSVに保存
                    pivot_by_area_kind.to_csv(csv_buffer_pivot, encoding='utf-8-sig') # BOM付きUTF-8
                    
                    st.download_button(
                        label="地域別ピボットテーブルを CSV でダウンロード",
                        data=csv_buffer_pivot.getvalue().encode("utf-8-sig"), # BOM付きUTF-8
                        file_name=f"warnings_pivot_by_area_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
                        mime="text/csv"
                    )

                except Exception as e:
                    st.error(f"データ処理またはピボットテーブル作成エラー: {e}")
                    pivot_by_area_kind = pd.DataFrame() # エラー発生時は空のDataFrameを作成
            else:
                 st.warning("日付情報の変換に失敗したため、ピボットテーブルを作成できませんでした。")

    else:
        # この分岐は 'if parsed:' の中にあるので、通常はdfは存在するはずだが、念のため。
        st.info("ピボットテーブルを作成するためのデータがありません。")
    # --- ▲▲▲【改修されたコード2】のロジックをここまで追加 ▲▲▲ ---

else:
    st.info(f"{hours}時間以内に発表された '気象特別警報・警報・注意報' はありません。")

