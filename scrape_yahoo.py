# -*- coding: utf-8 -*-
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import psycopg2
from datetime import datetime, date
import re

SUPABASE_URL = os.environ.get("SUPABASE_DB_URL", "")

AREAS = {
    "東京":   "https://weather.yahoo.co.jp/weather/jp/13/4410.html",
    "大阪":   "https://weather.yahoo.co.jp/weather/jp/27/6200.html",
    "名古屋": "https://weather.yahoo.co.jp/weather/jp/23/5110.html",
    "札幌":   "https://weather.yahoo.co.jp/weather/jp/1b/1400/1101.html",
    "福岡":   "https://weather.yahoo.co.jp/weather/jp/40/8210.html",
    "仙台":   "https://weather.yahoo.co.jp/weather/jp/4/3410.html",
    "広島":   "https://weather.yahoo.co.jp/weather/jp/34/6710.html",
    "那覇":   "https://weather.yahoo.co.jp/weather/jp/47/9110.html",
    "金沢":   "https://weather.yahoo.co.jp/weather/jp/17/5610.html",
    "鹿児島": "https://weather.yahoo.co.jp/weather/jp/46/8810.html",
}

def parse_date(txt):
    m = re.search(r"(\d+)月(\d+)日", txt)
    if m:
        today = date.today()
        month, day = int(m.group(1)), int(m.group(2))
        year = today.year if month >= today.month else today.year + 1
        return f"{year}-{month:02d}-{day:02d}"
    return None

def scrape_area(driver, area_name, url):
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "yjw_table"))
        )
    except Exception:
        print(f"  タイムアウト")
        return []

    collected_at = datetime.now().isoformat(timespec="seconds")
    results = []

    try:
        table = driver.find_element(By.CLASS_NAME, "yjw_table")
        rows = table.find_elements(By.TAG_NAME, "tr")

        dates = []
        weathers = []
        temp_maxs = []
        temp_mins = []
        precips = []

        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue
            label = cells[0].text.strip()
            data_cells = cells[1:]

            if "日付" in label:
                for td in data_cells:
                    d = parse_date(td.text.strip())
                    if d:
                        dates.append(d)

            elif "天気" in label:
                for td in data_cells:
                    imgs = td.find_elements(By.TAG_NAME, "img")
                    weathers.append(imgs[0].get_attribute("alt") if imgs else td.text.strip())

            elif "気温" in label:
                for td in data_cells:
                    nums = re.findall(r"-?\d+", td.text.strip())
                    temp_maxs.append(int(nums[0]) if len(nums) >= 1 else None)
                    temp_mins.append(int(nums[1]) if len(nums) >= 2 else None)

            elif "降水" in label:
                for td in data_cells:
                    txt = re.sub(r"[^\d]", "", td.text.strip())
                    precips.append(int(txt) if txt else None)

        for i, d in enumerate(dates):
            results.append({
                "collected_at": collected_at,
                "target_date":  d,
                "area_name":    area_name,
                "source":       "yahoo",
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
    options.add_argument("--window-size=1920,3000")
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
