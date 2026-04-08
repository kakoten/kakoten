# -*- coding: utf-8 -*-
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time
import psycopg2
from datetime import datetime, date
import re

SUPABASE_URL = os.environ.get("SUPABASE_DB_URL", "")

AREAS = {
    "東京":   "https://tenki.jp/forecast/3/16/4410/13101/",
    "大阪":   "https://tenki.jp/forecast/6/30/6200/27100/",
    "名古屋": "https://tenki.jp/forecast/5/26/5110/23100/",
    "札幌":   "https://tenki.jp/forecast/1/2/1400/1100/",
    "福岡":   "https://tenki.jp/forecast/9/43/8210/40130/",
    "仙台":   "https://tenki.jp/forecast/2/7/3410/4100/",
    "広島":   "https://tenki.jp/forecast/7/37/6710/34100/",
    "那覇":   "https://tenki.jp/forecast/10/50/9110/47201/",
    "金沢":   "https://tenki.jp/forecast/4/20/5610/17201/",
    "鹿児島": "https://tenki.jp/forecast/9/49/8810/46201/",
}

def scrape_area(driver, area_name, url):
    driver.get(url)
    time.sleep(5)
    results = []
    collected_at = datetime.now().isoformat(timespec="seconds")

    try:
        table = driver.find_element(By.CLASS_NAME, "forecast-days-long")
        rows = table.find_elements(By.TAG_NAME, "tr")

        dates = []
        weathers = []
        temp_maxs = []
        temp_mins = []
        precips = []

        for row in rows:
            headers = row.find_elements(By.TAG_NAME, "th")
            if not headers:
                continue
            label = headers[0].text.strip()
            cells = row.find_elements(By.TAG_NAME, "td")

            if "日付" in label:
                for td in cells:
                    date_p = td.find_elements(By.CLASS_NAME, "date-box")
                    if date_p:
                        txt = date_p[0].text.strip()
                        m = re.search(r"(\d+)月(\d+)日", txt)
                        if m:
                            today = date.today()
                            month, day = int(m.group(1)), int(m.group(2))
                            year = today.year if month >= today.month else today.year + 1
                            dates.append(f"{year}-{month:02d}-{day:02d}")

            elif "天気" in label:
                for td in cells:
                    weathers.append(td.text.strip())

            elif "気温" in label:
                for td in cells:
                    highs = td.find_elements(By.CSS_SELECTOR, "p.high-temp")
                    lows  = td.find_elements(By.CSS_SELECTOR, "p.low-temp")
                    temp_maxs.append(int(highs[0].text.strip()) if highs else None)
                    temp_mins.append(int(lows[0].text.strip())  if lows  else None)

            elif "降水" in label:
                for td in cells:
                    p = td.find_elements(By.CSS_SELECTOR, "p.precip")
                    if p:
                        txt = p[0].text.replace("%", "").strip()
                        precips.append(int(txt) if txt.isdigit() else None)
                    else:
                        precips.append(None)

        for i, d in enumerate(dates):
            results.append({
                "collected_at": collected_at,
                "target_date":  d,
                "area_name":    area_name,
                "source":       "tenki",
                "weather_text": weathers[i]  if i < len(weathers)  else None,
                "precip_prob":  precips[i]   if i < len(precips)   else None,
                "temp_max":     temp_maxs[i] if i < len(temp_maxs) else None,
                "temp_min":     temp_mins[i] if i < len(temp_mins) else None,
            })

    except Exception as e:
        print(f"  ERROR: {e}")

    return results


def main():
    print("Supabase接続中...")
    pg = psycopg2.connect(SUPABASE_URL)
    pg.autocommit = False
    cur = pg.cursor()
    print("接続成功")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)

    total = 0
    for area_name, url in AREAS.items():
        print(f"  {area_name} 取得中...", end=" ")
        records = scrape_area(driver, area_name, url)

        for r in records:
            try:
                cur.execute("""
                    INSERT INTO forecasts
                        (collected_at, target_date, area_name, source,
                         weather_text, precip_prob, temp_max, temp_min)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    r["collected_at"], r["target_date"], r["area_name"], r["source"],
                    r["weather_text"], r["precip_prob"], r["temp_max"], r["temp_min"]
                ))
                total += 1
            except Exception as e:
                print(f"\n  INSERT失敗: {e}")
                pg.rollback()

        pg.commit()
        print(f"{len(records)}件保存")

    driver.quit()
    pg.close()
    print(f"完了: 合計{total}件保存")


if __name__ == "__main__":
    main()
