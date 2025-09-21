import os
import requests
from notion_client import Client
import base64
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import json
import time
import re
from firebase_admin import credentials, initialize_app, firestore

# --- 配置部分 ---
# 从GitHub Actions Secrets获取环境变量
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN")
# 这是一个逗号分隔的邮箱地址列表
# 修复了当环境变量不存在时的错误，并提供了默认值
GMAIL_RECIPIENT_EMAILS = os.environ.get("GMAIL_RECIPIENT_EMAILS", "tgllres@gmail.com").split(',')

# 从环境变量中获取Firebase配置
FIREBASE_CONFIG_JSON = os.environ.get("FIREBASE_CONFIG_JSON")
# 这两个变量在Canvas环境中会自动提供
APP_ID = os.environ.get("__app_id")
FIREBASE_CONFIG = os.environ.get("__firebase_config")

# --- Gemini API配置 ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# --- 初始化客户端 ---
# 初始化Notion客户端
notion = Client(auth=NOTION_TOKEN)

# 初始化Firebase Admin SDK
# 如果在Canvas环境外运行，需要使用`FIREBASE_CONFIG_JSON`
# 如果在Canvas环境内运行，则此段代码不会执行，因为Canvas已配置
if not FIREBASE_CONFIG:
    try:
        cred = credentials.Certificate(json.loads(FIREBASE_CONFIG_JSON))
        initialize_app(cred)
        db = firestore.client()
        print("Firebase Admin SDK 初始化成功。")
    except Exception as e:
        print(f"Firebase Admin SDK 初始化失败: {e}")
        db = None
else:
    # 在Canvas环境中，我们不使用Admin SDK，只准备好数据路径
    db = None

# --- Gmail API认证 ---
def get_gmail_service():
    """使用OAuth2凭证获取Gmail服务实例"""
    creds = Credentials(
        token=None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        client_id=GMAIL_CLIENT_ID,
        client_secret=GMAIL_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    # 使用刷新令牌获取新的访问令牌
    creds.refresh(Request())
    service = build("gmail", "v1", credentials=creds)
    return service

def create_message(sender, to, subject, message_text, subtype="plain"):
    """创建一个MIMEText消息对象，支持指定子类型"""
    message = MIMEText(message_text, subtype)
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_email_notification(to_list, subject, message_text, is_html=False):
    """使用Gmail API发送邮件"""
    try:
        service = get_gmail_service()
        for to in to_list:
            # 'me' 指的是经过身份验证的用户邮箱地址
            message = create_message("me", to.strip(), subject, message_text, "html" if is_html else "plain")
            service.users().messages().send(userId="me", body=message).execute()
            print(f"成功发送邮件至: {to.strip()}")
    except Exception as e:
        print(f"发送邮件失败: {e}")

# --- AI分析部分 (使用Gemini API) ---
def fetch_and_analyze_news():
    """
    使用Gemini API同时完成新闻爬取和分析任务
    """
    # 调整提示词以获得更稳定的JSON输出
    # 1. 明确要求dailyCommentary为格式化文本
    # 2. 明确要求usTop10Stocks, hkTop10Stocks, cnTop10Stocks中每个股票都包含价格、市值、周/月涨跌幅
    prompt_text = "你是一名资深金融分析师，拥有对美股、港股和中国沪深股市的深度分析能力。请根据你的知识库和可联网搜索到的过去一周的财经新闻和市场数据，完成以下分析任务。首先，从主流财经媒体和通讯社中获取最新的市场动态、政策变化和公司财报新闻。在获取了这些信息后，请完成以下分析：1. 整体市场情绪和摘要：给出对整体市场情绪的判断（利好、利空或中性），并提供一份整体行情摘要。2. 每周点评与预判：给出对美股、港股和大陆股市的专业点评和对后续走势的预判。请将此部分内容格式化为清晰的文本，用“美股市场点评：”等标题区分。3. 中长线投资推荐：选出美股、港股和中国沪深股市各10个值得中长线买入的股票代码，并为每个推荐给出对应的公司中文名称、当前股票价格、市值、市盈率、市净率、市销率、资产回报率以及过去一周和过去一个月的涨跌情况。同时，为每个推荐给出简短的入选理由（**每个理由请控制在200字以内**）。请将所有分析结果以严格的JSON格式返回，确保可直接解析。JSON对象的结构如下：{\"overallSentiment\": \"利好\",\"overallSummary\": \"...\",\"dailyCommentary\": \"...\",\"usTop10Stocks\": [{\"stockCode\": \"AAPL\",\"companyName\": \"苹果公司\",\"price\": \"...\","marketCap\": \"...\","peRatio": \"...\","psRatio": \"...\","roeRatio": \"...\","pbRatio": \"...\","weeklyChange\": \"...\","monthlyChange\": \"...\","reason\": \"...\"},...],\"hkTop10Stocks\": [{\"stockCode\": \"700.HK\",\"companyName\": \"腾讯控股\",\"price\": \"...\","marketCap\": \"...\","peRatio": \"...\","psRatio": \"...\","roeRatio": \"...\","pbRatio": \"...\","weeklyChange\": \"...\","monthlyChange\": \"...\","reason\": \"...\"},...],\"cnTop10Stocks\": [{\"stockCode\": \"600519.SH\",\"companyName\": \"贵州茅台\",\"price\": \"...\","marketCap\": \"...\","peRatio": \"...\","psRatio": \"...\","roeRatio": \"...\","pbRatio": \"...\","weeklyChange\": \"...\","monthlyChange\": \"...\","reason\": \"...\"},...]}}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "tools": [{"google_search": {}}] # 启用Google搜索工具
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={GEMINI_API_KEY}"
    
    print("开始调用 Gemini API...")
    print(f"API URL: {api_url}")
    print(f"请求负载: {json.dumps(payload, indent=2)}")
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # 检查HTTP错误
        
        result_json = response.json()
        raw_text = result_json['candidates'][0]['content']['parts'][0]['text']
        print("成功从 Gemini API 获取响应。")
        print(f"原始响应文本: {raw_text}")
        
        # 尝试从原始文本中提取JSON对象
        json_start_index = raw_text.find('{')
        json_end_index = raw_text.rfind('}')
        
        if json_start_index != -1 and json_end_index != -1 and json_end_index > json_start_index:
            json_text = raw_text[json_start_index:json_end_index + 1]
            print("成功提取JSON内容。")
        else:
            json_text = raw_text
            print("未能提取到有效的JSON块，将尝试解析整个响应。")
        
        try:
            analysis_data = json.loads(json_text)
            print("成功解析 JSON 数据。")
        except json.JSONDecodeError as e:
            print(f"解析 JSON 失败: {e}")
            send_email_notification(GMAIL_RECIPIENT_EMAILS, "理财分析任务失败", f"解析 JSON 失败: {e}\n\n原始文本:\n{raw_text}")
            return # 退出函数
        
        # --- 将分析结果写入Firestore ---
        if db:
            doc_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('finance_reports').document('latest')
            doc_ref.set(analysis_data)
            print("成功将数据写入Firestore。")

        # 写入Notion数据库
        title = f"每周金融分析报告 - {datetime.now().strftime('%Y-%m-%d')}"
        link = "N/A" # 综合报告没有单一链接
        
        # --- 处理股票推荐数据以适应Notion富文本限制 ---
        def process_stocks_for_notion(stocks_list, max_len=1900):
            """将股票推荐列表转换为JSON字符串并确保长度不超过max_len"""
            json_str = json.dumps(stocks_list, indent=2, ensure_ascii=False)
            if len(json_str) > max_len:
                print(f"股票列表JSON字符串长度过长（{len(json_str)}），正在截断...")
                truncated_str = json_str[:max_len-5] + '...' + json_str[-2:]
                return truncated_str
            return json_str

        us_stocks_notion = process_stocks_for_notion(analysis_data['usTop10Stocks'])
        hk_stocks_notion = process_stocks_for_notion(analysis_data['hkTop10Stocks'])
        cn_stocks_notion = process_stocks_for_notion(analysis_data['cnTop10Stocks'])

        write_to_notion(title, link, analysis_data['overallSentiment'], analysis_data['overallSummary'], analysis_data['dailyCommentary'],
                        us_stocks_notion, hk_stocks_notion, cn_stocks_notion)

        # --- 生成HTML邮件内容 ---
        def generate_html_email_body(data):
            """生成HTML格式的邮件正文"""
            def get_change_color(change):
                """根据涨跌幅返回颜色"""
                if isinstance(change, (int, float)):
                    if change > 0:
                        return "#16a34a"  # 绿色
                    elif change < 0:
                        return "#dc2626"  # 红色
                return "#4b5563" # 中性色

            def format_stocks_html(stocks, market_name):
                """生成股票推荐的HTML内容"""
                html = f'<div><h3 style="font-size: 1.25rem; font-weight: bold; color: #111827; margin-bottom: 1rem;">{market_name}</h3>'
                for s in stocks:
                    weekly_change = s.get('weeklyChange', 'N/A')
                    monthly_change = s.get('monthlyChange', 'N/A')
                    
                    # 格式化涨跌幅
                    weekly_change_str = f'{weekly_change}%' if isinstance(weekly_change, (int, float)) else weekly_change
                    monthly_change_str = f'{monthly_change}%' if isinstance(monthly_change, (int, float)) else monthly_change
                    
                    html += f"""
                    <div style="background-color:#f9fafb;border-radius:8px;padding:1rem;box-shadow:0 1px 2px rgba(0,0,0,0.05);margin-bottom:1rem;">
                        <h4 style="font-size:1.125rem;font-weight:600;color:#111827;margin-bottom:0.5rem;">{s['companyName']} ({s['stockCode']})</h4>
                        <ul style="list-style:none;padding:0;margin:0;font-size:0.875rem;color:#4b5563;">
                            <li style="margin-bottom:0.25rem;"><strong>价格:</strong> {s.get('price', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>市值:</strong> {s.get('marketCap', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>市盈率 (PE):</strong> {s.get('peRatio', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>市净率 (PB):</strong> {s.get('pbRatio', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>市销率 (PS):</strong> {s.get('psRatio', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>资产回报率 (ROE):</strong> {s.get('roeRatio', 'N/A')}</li>
                            <li style="margin-bottom:0.25rem;"><strong>周涨跌:</strong> <span style="color:{get_change_color(s.get('weeklyChange'))};">{weekly_change_str}</span></li>
                            <li style="margin-bottom:0.25rem;"><strong>月涨跌:</strong> <span style="color:{get_change_color(s.get('monthlyChange'))};">{monthly_change_str}</span></li>
                        </ul>
                        <p style="margin-top:0.75rem;font-size:0.875rem;color:#4b5563;"><strong>推荐理由:</strong> {s['reason']}</p>
                    </div>
                    """
                html += '</div>'
                return html

            us_stocks_html = format_stocks_html(data['usTop10Stocks'], '美股 (US)')
            hk_stocks_html = format_stocks_html(data['hkTop10Stocks'], '港股 (HK)')
            cn_stocks_html = format_stocks_html(data['cnTop10Stocks'], '沪深股市 (CN)')
            
            sentiment_color_map = {'利好': '#dcfce7', '利空': '#fee2e2', '中性': '#fef9c3'}
            sentiment_text_color_map = {'利好': '#16a34a', '利空': '#dc2626', '中性': '#ca8a04'}
            sentiment_bg = sentiment_color_map.get(data['overallSentiment'], '#f3f4f6')
            sentiment_text = sentiment_text_color_map.get(data['overallSentiment'], '#374151')

            html_content = f"""
            <html>
            <body style="font-family: 'Inter', sans-serif; background-color: #f9fafb; padding: 2rem;">
                <div style="max-width: 800px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); padding: 2rem;">
                    <h1 style="text-align: center; font-size: 2.25rem; font-weight: 800; color: #111827; margin-bottom: 0.5rem;">每周金融分析报告</h1>
                    <p style="text-align: center; font-size: 1.125rem; color: #4b5563; margin-bottom: 2rem;">由 Gemini AI 自动生成</p>

                    <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 2rem;">
                        <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">整体市场情绪</h2>
                        <div style="font-size: 2.25rem; font-weight: bold; border-radius: 8px; padding: 1rem; margin-top: 1rem; text-align: center; background-color: {sentiment_bg}; color: {sentiment_text};">
                            {data['overallSentiment']}
                        </div>
                    </div>

                    <div style="display: grid; grid-template-columns: 1fr; gap: 2rem; margin-bottom: 2rem;">
                        <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                            <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">整体摘要</h2>
                            <p style="margin-top: 1rem; color: #374151; line-height: 1.5;">{data['overallSummary']}</p>
                        </div>
                        <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                            <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">每周点评</h2>
                            <p style="margin-top: 1rem; color: #374151; line-height: 1.5;">{data['dailyCommentary'].replace('\\n', '<br>')}</p>
                        </div>
                    </div>

                    <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                        <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">中长线投资推荐</h2>
                        <div style="display: grid; grid-template-columns: 1fr; gap: 1.5rem; margin-top: 1.5rem;">
                            {us_stocks_html}
                            {hk_stocks_html}
                            {cn_stocks_html}
                        </div>
                    </div>
                </div>
            </body>
            </html>
            """
            return html_content

        # 生成HTML邮件正文
        email_html_body = generate_html_email_body(analysis_data)
        
        # 发送邮件通知，现在使用HTML格式
        email_subject = f"【理财分析】每周报告 - {datetime.now().strftime('%Y-%m-%d')}"
        send_email_notification(GMAIL_RECIPIENT_EMAILS, email_subject, email_html_body, is_html=True)

    except Exception as e:
        print(f"Gemini调用或分析失败: {e}")
        send_email_notification(GMAIL_RECIPIENT_EMAILS, "理财分析任务失败", f"Gemini调用或分析失败：{e}")


# --- Notion数据库写入函数 ---
def write_to_notion(title, url, overallSentiment, overallSummary, dailyCommentary, usTop10Stocks, hkTop10Stocks, cnTop10Stocks):
    """将数据写入Notion数据库"""
    try:
        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": title}}]},
                "URL": {"url": url},
                "OverallSentiment": {"select": {"name": overallSentiment}},
                "OverallSummary": {"rich_text": [{"text": {"content": overallSummary}}]},
                "DailyCommentary": {"rich_text": [{"text": {"content": dailyCommentary}}]},
                "usTop10Stocks": {"rich_text": [{"text": {"content": usTop10Stocks}}]},
                "hkTop10Stocks": {"rich_text": [{"text": {"content": hkTop10Stocks}}]},
                "cnTop10Stocks": {"rich_text": [{"text": {"content": cnTop10Stocks}}]},
                "CrawledDate": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        print(f"成功写入Notion：{title}")
    except Exception as e:
        print(f"写入Notion失败：{e}")

if __name__ == "__main__":
    fetch_and_analyze_news()
