# -*- coding: utf-8 -*-
import json
import os
import psycopg2
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_DB_URL", "postgresql://postgres.ruozldlqsdelhnlorvmq:Kakoten2026@aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres")

AREAS = {
    "東京":    {"forecast_code": "130000", "amedas_point": "44132"},
    "大阪":    {"forecast_code": "270000", "amedas_point": "62078"},
    "名古屋":  {"forecast_code": "230000", "amedas_point": "51106"},
    "札幌":    {"forecast_code": "016000", "amedas_point": "14163"},
    "福岡":    {"forecast_code": "400000", "amedas_point": "82182"},
    "仙台":    {"forecast_code": "040000", "amedas_point": "34392"},
    "広島":    {"forecast_code": "340000", "amedas_point": "67437"},
    "那覇":    {"forecast_code": "471000", "amedas_point": "91197"},
    "金沢":    {"forecast_code": "170000", "amedas_point": "56227"},
    "鹿児島":  {"forecast_code": "460100", "amedas_point": "88317"},
}

def fetch_jma_forecast(area_code):
    url = f"https://www.jma.go.jp/bosai/forecast/data/forecast/{area_code}.json"
    try:
        with urllib.request.urlopen(url, timeout=10) as res:
            return json.loads(res.read())
    except urllib.error.URLError as e:
        print(f"  取得失敗: {e}")
        return None

def parse_jma_forecast(data, area_name):
    records = []
    collected_at = datetime.now().isoformat(timespec="seconds")
    seen_dates = set()

    short_ts = data[0].get("timeSeries", []) if len(data) > 0 else []
    if short_ts:
        weather_series = short_ts[0] if len(short_ts) > 0 else {}
        precip_series  = short_ts[1] if len(short_ts) > 1 else {}
        temp_series    = short_ts[2] if len(short_ts) > 2 else {}
        weather_times = weather_series.get("timeDefines", [])
        precip_times  = precip_series.get("timeDefines", [])
        temp_times    = temp_series.get("timeDefines", [])
        w_area = weather_series.get("areas", [{}])[0]
        p_area = precip_series.get("areas", [{}])[0]
        t_area = temp_series.get("areas", [{}])[0]
        weathers   = w_area.get("weatherCodes", [])
        weather_tx = w_area.get("weathers", [])
        precips    = p_area.get("pops", [])
        temps      = t_area.get("temps", [])
        for i, time_str in enumerate(weather_times):
            target_date = time_str[:10]
            if target_date in seen_dates:
                continue
            day_precips = []
            for j, pt in enumerate(precip_times):
                if pt[:10] == target_date and j < len(precips) and precips[j] != "":
                    try:
                        day_precips.append(int(precips[j]))
                    except ValueError:
                        pass
            precip_prob = max(day_precips) if day_precips else None
            temp_max = temp_min = None
            for j, tt in enumerate(temp_times):
                if tt[:10] == target_date and j < len(temps) and temps[j] != "":
                    try:
                        v = float(temps[j])
                        if temp_max is None:
                            temp_max = v
                        else:
                            temp_min = min(temp_max, v)
                            temp_max = max(temp_max, v)
                    except ValueError:
                        pass
            records.append({
                "collected_at": collected_at, "target_date": target_date,
                "area_name": area_name, "source": "jma",
                "weather_code": weathers[i] if i < len(weathers) else None,
                "weather_text": weather_tx[i].replace("\u3000", " ") if i < len(weather_tx) else None,
                "precip_prob": precip_prob, "temp_max": temp_max, "temp_min": temp_min,
            })
            seen_dates.add(target_date)

    weekly_ts = data[1].get("timeSeries", []) if len(data) > 1 else []
    if weekly_ts:
        weather_series = weekly_ts[0] if len(weekly_ts) > 0 else {}
        temp_series    = weekly_ts[1] if len(weekly_ts) > 1 else {}
        weather_times = weather_series.get("timeDefines", [])
        w_area = weather_series.get("areas", [{}])[0]
        t_area = temp_series.get("areas", [{}])[0]
        weathers   = w_area.get("weatherCodes", [])
        weather_tx = w_area.get("weathers", [])
        precips    = w_area.get("precipitationProbability", [])
        temps_max  = t_area.get("tempsMax", [])
        temps_min  = t_area.get("tempsMin", [])
        for i, time_str in enumerate(weather_times):
            target_date = time_str[:10]
            if target_date in seen_dates:
                continue
            precip_prob = None
            if i < len(precips) and precips[i] != "":
                try:
                    precip_prob = int(precips[i])
                except ValueError:
                    pass
            temp_max = None
            if i < len(temps_max) and temps_max[i] != "":
                try:
                    temp_max = float(temps_max[i])
                except ValueError:
                    pass
            temp_min = None
            if i < len(temps_min) and temps_min[i] != "":
                try:
                    temp_min = float(temps_min[i])
                except ValueError:
                    pass
            records.append({
                "collected_at": collected_at, "target_date": target_date,
                "area_name": area_name, "source": "jma",
                "weather_code": weathers[i] if i < len(weathers) else None,
                "weather_text": weather_tx[i].replace("\u3000", " ") if i < len(weather_tx) else None,
                "precip_prob": precip_prob, "temp_max": temp_max, "temp_min": temp_min,
            })
            seen_dates.add(target_date)
    return records

def collect_forecasts(pg):
    print(f"予報収集開始: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    total = 0
    cur = pg.cursor()
    for area_name, cfg in AREAS.items():
        print(f"  {area_name} 取得中...", end=" ")
        data = fetch_jma_forecast(cfg["forecast_code"])
        if data is None:
            print("スキップ")
            continue
        records = parse_jma_forecast(data, area_name)
        if records is None:
            continue
        for r in records:
            try:
                cur.execute("""
                    INSERT INTO forecasts (collected_at, target_date, area_name, source, weather_code, weather_text, precip_prob, temp_max, temp_min)
                    VALUES (%(collected_at)s, %(target_date)s, %(area_name)s, %(source)s, %(weather_code)s, %(weather_text)s, %(precip_prob)s, %(temp_max)s, %(temp_min)s)
                    ON CONFLICT DO NOTHING
                """, r)
                total += 1
            except Exception as e:
                print(f"\n  INSERT失敗: {e}")
                pg.rollback()
        pg.commit()
        print(f"{len(records)}件保存")
    cur.close()
    print(f"合計: {total} 件保存")

def fetch_amedas_daily(point_code, target_date):
    date_str = target_date.strftime("%Y%m%d")
    url = f"https://www.jma.go.jp/bosai/amedas/data/point/{point_code}/{date_str}_00.json"
    try:
        with urllib.request.urlopen(url, timeout=10) as res:
            return json.loads(res.read())
    except urllib.error.URLError as e:
        print(f"  AMeDAS取得失敗: {e}")
        return None

def parse_amedas_daily(data):
    temps = []
    precips = []
    for time_key, obs in data.items():
        if "temp" in obs and obs["temp"][1] == 0:
            temps.append(obs["temp"][0])
        if "precipitation10m" in obs and obs["precipitation10m"][1] == 0:
            precips.append(obs["precipitation10m"][0])
    return {
        "temp_max": max(temps) if temps else None,
        "temp_min": min(temps) if temps else None,
        "precip_mm": sum(precips) if precips else None,
    }

def verify_actuals(pg, target_date=None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)
    date_str = target_date.strftime("%Y-%m-%d")
    print(f"実績検証: {date_str}")
    cur = pg.cursor()
    for area_name, cfg in AREAS.items():
        print(f"  {area_name}...", end=" ")
        cur.execute("SELECT id FROM actuals WHERE target_date=%s AND area_name=%s", (date_str, area_name))
        if cur.fetchone():
            print("スキップ")
            continue
        data = fetch_amedas_daily(cfg["amedas_point"], target_date)
        if data is None:
            print("データなし")
            continue
        actual = parse_amedas_daily(data)
        weather_text = "雨" if (actual["precip_mm"] or 0) >= 1.0 else "晴れ/曇り"
        cur.execute("""
            INSERT INTO actuals (target_date, area_name, weather_text, precip_mm, temp_max, temp_min, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_date, area_name) DO UPDATE
            SET weather_text=EXCLUDED.weather_text, precip_mm=EXCLUDED.precip_mm,
                temp_max=EXCLUDED.temp_max, temp_min=EXCLUDED.temp_min
        """, (date_str, area_name, weather_text, actual["precip_mm"], actual["temp_max"], actual["temp_min"], "jma_amedas"))
        _calc_scores(cur, date_str, area_name, actual)
        pg.commit()
        print(f"最高{actual['temp_max']}C 最低{actual['temp_min']}C 降水{actual['precip_mm']}mm")
    cur.close()

def _calc_scores(cur, date_str, area_name, actual):
    cur.execute("SELECT source, precip_prob, temp_max, temp_min FROM forecasts WHERE target_date=%s AND area_name=%s", (date_str, area_name))
    forecasts = cur.fetchall()
    actual_rain = (actual["precip_mm"] or 0) >= 1.0
    for source, precip_prob, f_temp_max, f_temp_min in forecasts:
        precip_hit = None
        if precip_prob is not None:
            precip_hit = 1 if ((precip_prob >= 50) == actual_rain) else 0
        temp_max_err = abs(f_temp_max - actual["temp_max"]) if (f_temp_max and actual["temp_max"]) else None
        temp_min_err = abs(f_temp_min - actual["temp_min"]) if (f_temp_min and actual["temp_min"]) else None
        score = 0
        count = 0
        if precip_hit is not None:
            score += precip_hit * 50; count += 1
        if temp_max_err is not None:
            score += max(0, 25 - temp_max_err * 5); count += 1
        if temp_min_err is not None:
            score += max(0, 25 - temp_min_err * 5); count += 1
        final_score = round(score, 2) if count > 0 else None
        cur.execute("""
            INSERT INTO scores (target_date, area_name, source, precip_hit, temp_max_error, temp_min_error, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_date, area_name, source) DO UPDATE
            SET precip_hit=EXCLUDED.precip_hit, temp_max_error=EXCLUDED.temp_max_error,
                temp_min_error=EXCLUDED.temp_min_error, score=EXCLUDED.score
        """, (date_str, area_name, source, precip_hit,
              round(temp_max_err, 4) if temp_max_err else None,
              round(temp_min_err, 4) if temp_min_err else None,
              final_score))

def main():
    import sys
    print("Supabase接続中...")
    try:
        pg = psycopg2.connect(SUPABASE_URL)
        pg.autocommit = False
        print("接続成功")
    except Exception as e:
        print(f"接続失敗: {e}")
        return
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "collect":
        collect_forecasts(pg)
    elif cmd == "verify":
        d = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else None
        verify_actuals(pg, d)
    pg.close()

if __name__ == "__main__":
    main()
