import Anthropic from '@anthropic-ai/sdk';
import { createClient } from '@supabase/supabase-js';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  const dateStr = tomorrow.toISOString().split('T')[0];

  const { data: forecasts } = await supabase
    .from('forecasts')
    .select('source, weather_text, precip_prob, temp_max, temp_min')
    .eq('target_date', dateStr)
    .eq('area_name', '東京')
    .order('collected_at', { ascending: false });

  const forecastText = forecasts?.map(f =>
    `・${f.source}: 天気=${f.weather_text||'不明'} 降水確率=${f.precip_prob||'--'}% 最高=${f.temp_max||'--'}℃`
  ).join('\n') || 'データなし';

  const message = await anthropic.messages.create({
    model: 'claude-haiku-4-5',
    max_tokens: 300,
    messages: [{
      role: 'user',
      content: `気象予報の専門家として、以下の${dateStr}東京の予報データを分析してください。\n\n${forecastText}\n\n【かこてん総合予報】と【今日のポイント】を簡潔に日本語で。`
    }]
  });

  res.json({ result: message.content[0].text, date: dateStr });
}
