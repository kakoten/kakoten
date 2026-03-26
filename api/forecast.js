import Anthropic from '@anthropic-ai/sdk';
import { createClient } from '@supabase/supabase-js';
 
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);
 
function markdownToHtml(text) {
  return text
    .replace(/^## (.+)$/gm, '<strong>$1</strong>')
    .replace(/^# (.+)$/gm, '<strong>$1</strong>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br>');
}
 
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
 
  try {
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const dateStr = tomorrow.toISOString().split('T')[0];
 
    // 予報データ取得（DISTINCT ON の代わりに全件取得して絞る）
    const { data: allForecasts, error } = await supabase
      .from('forecasts')
      .select('source, weather_text, precip_prob, temp_max, temp_min, collected_at')
      .eq('target_date', dateStr)
      .eq('area_name', '東京')
      .order('collected_at', { ascending: false });
 
    if (error) {
      return res.status(500).json({ error: error.message });
    }
 
    // ソースごとに最新1件だけ残す
    const latest = {};
    for (const row of (allForecasts || [])) {
      if (!latest[row.source]) latest[row.source] = row;
    }
    const forecasts = Object.values(latest);
 
    const forecastText = forecasts.length > 0
      ? forecasts.map(f =>
          `・${f.source}: 天気=${f.weather_text || '不明'} 降水確率=${f.precip_prob ?? '--'}% 最高=${f.temp_max ?? '--'}℃ 最低=${f.temp_min ?? '--'}℃`
        ).join('\n')
      : 'データなし';
 
    const message = await anthropic.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `気象予報の専門家として、以下の${dateStr}東京の予報データを分析してください。\n\n${forecastText}\n\n【かこてん総合予報】と【今日のポイント】を簡潔な日本語で。マークダウン記法（#や**）は使わず、プレーンテキストで答えてください。`
      }]
    });
 
    const rawText = message.content[0].text;
    const html = markdownToHtml(rawText);
 
    res.json({ result: html, date: dateStr, sources: forecasts.length });
 
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
 
