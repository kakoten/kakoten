
# -*- coding: utf-8 -*-
import os
import psycopg2
import sys
import anthropic
import requests
from requests_oauthlib import OAuth1
from datetime import date, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_DB_URL", "")
API_KEY = os.environ.get("TWITTER_API_KEY", "")
API_SECRET = os.environ.get("TWITTER_API_SECRET", "")
ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN", "")
ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

WEATHER_CODE_TEXT = {
    100: '晴れ', 101: '晴れ時々曇', 102: '晴れ一時雨', 103: '晴れ時々雨',
    110: '晴れのち曇', 111: '晴れのち曇', 112: '晴れのち雨', 113: '晴れのち雨',
    200: '曇り', 201: '曇り時々晴', 202: '曇り一時雨', 203: '曇り時々雨',
    204: '曇り一時雪', 205: '曇り時々雪', 209: '霧',
    210: '曇りのち晴', 211: '曇りのち晴', 212: '曇りのち雨', 213: '曇りのち雨',
    300: '雨', 301: '雨時々晴', 302: '雨時々止む', 303: '雨時々雪',
    311: '雨のち晴', 313: '雨のち曇',
    400: '雪', 401: '雪時々晴', 402: '雪時々止む',
    411: '雪のち晴', 413: '雪のち曇',
}

def weather_code_to_text(code):
    if code is None:
        return None
    try:
        return WEATHER_CODE_TEXT.get(int(code))
    except (ValueError, TypeError):
        return None

def get_weather_text(w_code, w_text):
    if w_text and w_text.strip():
        t = w_text.strip()
        t = t.split()[0] if ' ' in t else t
        return t[:6]
    return weather_code_to_text(w_code)

def get_weather_emoji(code, text):
    if code:
        try:
            c = int(code)
            if 400 <= c < 500: return "❄️"
            if 300 <= c < 400: return "🌧️"
            if 200 <= c < 300: return "☁️"
            if 100 <= c < 200: return "☀️"
        except (ValueError, TypeError):
            pass
    if text:
        if "雨" in text: return "🌧️"
        if "雪" in text: return "❄️"
        if "曇" in text: return "☁️"
        if "晴" in text: return "☀️"
    return "🌡️"

def score_emoji(score):
    if score is None: return ""
    if score >= 85: return "🥇"
    if score >= 75: return "🥈"
    if score >= 65: return "🥉"
    return "💧"

def get_ai_forecast(target_date, forecasts):
    if not forecasts or not ANTHROPIC_API_KEY:
        return None
    date_str = target_date.strftime("%Y年%m月%d日")
    forecast_text = ""
    for src, w_code, w_text, prob, t_max, t_min in forecasts:
        display_text = get_weather_text(w_code, w_text) or '不明'
        forecast_text += f"・{src}: {display_text} {prob or '--'}% 最高{t_max or '--'}℃\n"

    prompt = f"""{date_str}東京の予報です。
{forecast_text}
一言予報を30文字以内で。テキストのみ。"""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=60,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text.strip()
    except Exception as e:
        print(f"AI予報エラー: {e}")
        return None

def generate_tweet(target_date: date) -> str:
    print("Supabaseに接続中...")
    conn = psycopg2.connect(SUPABASE_URL)
    print("接続成功！")

    cur = conn.cursor()
    date_str = target_date.strftime("%Y-%m-%d")
    date_display = f"{target_date.month}月{target_date.day}日"

    cur.execute(
        "SELECT weather_text, precip_mm, temp_max, temp_min FROM actuals WHERE target_date=%s AND area_name='東京'",
        (date_str,)
    )
    actual = cur.fetchone()

    cur.execute("""
        SELECT DISTINCT ON (source)
            source, weather_code, weather_text, precip_prob, temp_max, temp_min
        FROM forecasts
        WHERE target_date=%s AND area_name='東京'
        ORDER BY source, collected_at DESC
    """, (date_str,))
    forecasts = cur.fetchall()

    cur.execute("""
        SELECT source, score, precip_hit
        FROM scores
        WHERE target_date=%s AND area_name='東京'
    """, (date_str,))
    score_map = {s[0]: (s[1], s[2]) for s in cur.fetchall()}

    cur.close()
    conn.close()

    SOURCE_NAMES = {"jma": "気象庁", "tenki": "tenki", "yahoo": "Yahoo"}

    lines = []
    lines.append(f"【{date_display}の天気振り返り/東京】")
    lines.append("")

    if forecasts:
        lines.append("各社予報→結果")
        for src, w_code, w_text, prob, t_max, t_min in forecasts:
            name = SOURCE_NAMES.get(src, src)
            emoji = get_weather_emoji(w_code, w_text)
            prob_str = f"{prob}%" if prob is not None else "--"
            temp_str = f"{int(t_max)}℃" if t_max else "--"
            lines.append(f"{name} {emoji} {prob_str} 最高{temp_str}")

    lines.append("")

    if actual:
        a_text, a_mm, a_max, a_min = actual
        a_emoji = get_weather_emoji(None, a_text)
        mm_str = f"{a_mm:.1f}mm" if a_mm else "0mm"
        max_str = f"{int(a_max)}℃" if a_max else "--"
        lines.append(f"実際: {a_emoji} 降水{mm_str} 最高{max_str}")
    else:
        lines.append("実績データは翌朝取得されます")

    lines.append("")

    if score_map:
        lines.append("スコア")
        for src, (sc, hit) in sorted(score_map.items(), key=lambda x: x[1][0] or 0, reverse=True):
            name = SOURCE_NAMES.get(src, src)
            sc_str = f"{int(sc)}点" if sc is not None else "--"
            em = score_emoji(sc)
            lines.append(f"{em}{name}: {sc_str}")

    lines.append("")
    ai_forecast = get_ai_forecast(target_date, forecasts)
    if ai_forecast:
        lines.append(f"🤖{ai_forecast}")
        lines.append("")
    lines.append("#かこてん #天気予報")

    tweet = "\n".join(lines)
    char_count = sum(2 if ord(c) > 0x7F else 1 for c in tweet) // 2
    print(f"\n{'='*50}")
    print(tweet)
    print(f"{'='*50}")
    print(f"文字数: {char_count} / 140")

    return tweet

def post_tweet(text):
    auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    response = requests.post(
        "https://api.twitter.com/2/tweets",
        auth=auth,
        json={"text": text}
    )
    if response.status_code == 201:
        print("投稿成功！")
    else:
        print(f"投稿失敗: {response.status_code} {response.text}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    else:
        target = date.today() - timedelta(days=1)

    tweet = generate_tweet(target)
    post_tweet(tweet)
