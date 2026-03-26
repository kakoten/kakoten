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
    // クエリパラメータ対応（なければデフォルトで東京・明日）
    const area = req.query?.area || '東京';
    let dateStr = req.query?.date;
    if (!dateStr) {
      const tomorrow = new Date();
      tomorrow.setDate(tomorrow.getDate() + 1);
      dateStr = tomorrow.toISOString().split('T')[0];
    }

    const { data: allForecasts, error } = await supabase
      .from('forecasts')
      .select('source, weather_text, precip_prob, temp_max, temp_min, collected_at')
      .eq('target_date', dateStr)
      .eq('area_name', area)
      .order('collected_at', { ascending: false });

    if (error) {
      return res.status(500).json({ error: error.message });
    }

    // ソースごとに最新1件
    const latest = {};
    for (const row of (allForecasts || [])) {
      if (!latest[row.source]) latest[row.source] = row;
    }
    const forecasts = Object.values(latest);

    if (forecasts.length === 0) {
      return res.json({ result: null, date: dateStr, area });
    }

    const forecastText = forecasts.map(f =>
      `・${f.source}: 天気=${f.weather_text || '不明'} 降水確率=${f.precip_prob ?? '--'}% 最高=${f.temp_max ?? '--'}℃ 最低=${f.temp_min ?? '--'}℃`
    ).join('\n');

    const message = await anthropic.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `気象予報の専門家として、以下の${dateStr}の${area}の予報データを分析してください。\n\n${forecastText}\n\n【かこてん総合予報】と【今日のポイント】を簡潔な日本語で。マークダウン記法（#や**）は使わず、プレーンテキストで答えてください。`
      }]
    });

    const rawText = message.content[0].text;
    const html = markdownToHtml(rawText);

    res.json({ result: html, date: dateStr, area, sources: forecasts.length });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
