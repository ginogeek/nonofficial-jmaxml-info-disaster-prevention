import streamlit as st
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import pandas as pd
import io

KISHOU_XML_PAGE_URL = "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"

st.set_page_config(page_title="気象庁 防災情報 (XML) ビューア", layout="wide")

# --- 追加部分: 気象庁防災情報XMLの説明 ---
with st.expander("📘 気象庁防災情報XMLとは？", expanded=True):
    st.markdown("""
    気象庁は、**気象・津波・地震・火山などの防災情報**を迅速かつ正確に伝えるために  
    「気象庁防災情報XMLフォーマット」を策定し、2005年から運用しています。

    - **目的**: 自然災害の軽減、国民生活の向上、交通安全の確保、産業の発展を支援  
    - **特徴**:  
        - XML形式で機械可読な防災情報を提供  
        - ニュースや自治体システムなどでの自動処理・配信が可能  
        - 「Pull型」で誰でも自由に取得可能（ユーザー登録不要）  
    - **利用例**:  
        - 防災アプリや自治体システムでの自動通知  
        - 報道機関による速報配信  
        - 研究・教育分野でのデータ活用  

    詳細は [気象庁公式サイト](https://xml.kishou.go.jp/) をご参照ください。
    """)

@st.cache_data(ttl=600)
def fetch_feed(url: str, hours_threshold: int = 48):
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
                    if feed_report_time_str.endswith("Z"):
                        feed_report_time = datetime.fromisoformat(feed_report_time_str[:-1]).replace(tzinfo=timezone.utc)
                    else:
                        feed_report_time = datetime.fromisoformat(feed_report_time_str)
                    if feed_report_time < time_threshold:
                        skip_by_time = True
                except Exception:
                    pass

            linked_xml_link_element = entry.find(f'{atom_ns}link[@type="application/xml"]')
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
    parsed = []
    if not fetched_data or not fetched_data.get("linked_entries_xml"):
        return parsed

    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

    for entry in fetched_data["linked_entries_xml"]:
        feed_title = entry.get("FeedTitle", "N/A")
        if feed_title != "気象特別警報・警報・注意報":
            continue

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
        report_dt = extracted["FeedReportDateTime"]

        if linked_bytes:
            try:
                xml_text = linked_bytes.decode("utf-8")
            except Exception:
                xml_text = linked_bytes.decode("utf-8", errors="replace")
            try:
                root = ET.fromstring(xml_text)
                rt = root.find('.//{*}ReportDateTime')
                if rt is not None and rt.text:
                    report_dt = rt.text

                headline = root.find('.//{*}Headline/{*}Text')
                overall_detail = headline.text if headline is not None and headline.text else "N/A"

                items = root.findall('.//{*}Item')
                for item in items:
                    kind_el = item.find('.//{*}Kind/{*}Name')
                    area_el = item.find('.//{*}Areas/{*}Area/{*}Name')
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
            warnings.append({"Kind": "取得失敗", "Area": "取得失敗", "Detail": "リンクXMLがありません"})

        if warnings and extracted["LinkedXMLDataPresent"]:
            extracted["ReportDateTime"] = report_dt
            extracted["WarningsAdvisories
