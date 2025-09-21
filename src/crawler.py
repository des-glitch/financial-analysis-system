import os
import requests
from notion_client import Client
import base64
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import json
import time
import re
from firebase_admin import credentials, initialize_app, firestore

# --- Configuration ---
# Get environment variables from GitHub Actions Secrets
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN")

# Fix inconsistent environment variable name, also support a single email address
gmail_emails_str = os.environ.get("GMAIL_RECIPIENT_EMAILS") or os.environ.get("GMAIL_RECIPIENT_EMAIL")
if gmail_emails_str:
    GMAIL_RECIPIENT_EMAILS = [email.strip() for email in gmail_emails_str.split(',')]
else:
    GMAIL_RECIPIENT_EMAILS = []

# Get Firebase config from environment variables
FIREBASE_CONFIG_JSON = os.environ.get("FIREBASE_CONFIG_JSON")
# These two variables are automatically provided in the Canvas environment
APP_ID = os.environ.get("__app_id")
FIREBASE_CONFIG = os.environ.get("__firebase_config")

# --- Gemini API Configuration ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# --- Initialize clients ---
# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)

# Initialize Firebase Admin SDK
# If running outside the Canvas environment, use `FIREBASE_CONFIG_JSON`
# If running in the Canvas environment, this block will be skipped
if not FIREBASE_CONFIG:
    if FIREBASE_CONFIG_JSON:
        try:
            cred = credentials.Certificate(json.loads(FIREBASE_CONFIG_JSON))
            initialize_app(cred)
            db = firestore.client()
            print("Firebase Admin SDK initialized successfully.")
        except Exception as e:
            print(f"Failed to initialize Firebase Admin SDK: {e}")
            db = None
    else:
        print("FIREBASE_CONFIG_JSON environment variable not found. Firebase Admin SDK not initialized.")
        db = None
else:
    # In the Canvas environment, we don't use the Admin SDK, just prepare the data path
    db = None

# --- Gmail API authentication ---
def get_gmail_service():
    """Get a Gmail service instance using OAuth2 credentials"""
    creds = Credentials(
        token=None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        client_id=GMAIL_CLIENT_ID,
        client_secret=GMAIL_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    # Use the refresh token to get a new access token
    creds.refresh(Request())
    service = build("gmail", "v1", credentials=creds)
    return service

def create_message(sender, to, subject, message_text, subtype="plain"):
    """Create a MIMEText message object, supporting a specified subtype"""
    message = MIMEText(message_text, subtype)
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_email_notification(to_list, subject, message_text, is_html=False):
    """Send an email using the Gmail API"""
    if not to_list:
        print("No recipient emails specified, skipping email sending.")
        return
        
    try:
        service = get_gmail_service()
        for to in to_list:
            # 'me' refers to the authenticated user's email address
            message = create_message("me", to.strip(), subject, message_text, "html" if is_html else "plain")
            service.users().messages().send(userId="me", body=message).execute()
            print(f"Successfully sent email to: {to.strip()}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# --- Core logic function: Call AI and parse data ---
def _get_gemini_analysis():
    """Call the Gemini API and return the raw response text"""
    json_schema = {
        "overallSentiment": "利好",
        "overallSummary": "...",
        "dailyCommentary": "...",
        "relatedNewsLinks": [
            {
                "title": "...",
                "url": "..."
            }
        ],
        "usTop10Stocks": [
            {
                "stockCode": "AAPL",
                "companyName": "苹果公司",
                "price": "...",
                "marketCap": "...",
                "peRatio": "...",
                "psRatio": "...",
                "roeRatio": "...",
                "pbRatio": "...",
                "weeklyChange": "...",
                "monthlyChange": "...",
                "reason": "..."
            }
        ],
        "hkTop10Stocks": [
            {
                "stockCode": "700.HK",
                "companyName": "腾讯控股",
                "price": "...",
                "marketCap": "...",
                "peRatio": "...",
                "psRatio": "...",
                "roeRatio": "...",
                "pbRatio": "...",
                "weeklyChange": "...",
                "monthlyChange": "...",
                "reason": "..."
            }
        ],
        "cnTop10Stocks": [
            {
                "stockCode": "600519.SH",
                "companyName": "贵州茅台",
                "price": "...",
                "marketCap": "...",
                "peRatio": "...",
                "psRatio": "...",
                "roeRatio": "...",
                "pbRatio": "...",
                "weeklyChange": "...",
                "monthlyChange": "...",
                "reason": "..."
            }
        ]
    }
    
    # 优化后的提示词
    prompt_prefix = """
你是一名资深金融分析师。你必须严格根据可联网搜索到的过去一周（七天）的财经新闻和市场数据进行分析。

请完成以下分析任务：
1. **整体市场情绪和摘要**：给出对整体市场情绪的判断（利好、利空或中性），并提供一份整体行情摘要。
2. **每周点评与预判**：给出对美股、港股和大陆股市的专业点评和对后续走势的预判。请将此部分内容格式化为清晰的文本，用“美股市场点评：”等标题区分。
3. **中长线投资推荐**：选出美股、港股和中国沪深股市各10个值得中长线买入的股票代码，并为每个推荐给出对应的公司中文名称、市值、市盈率、市净率、市销率、资产回报率以及过去一周和过去一个月的涨跌情况。**请务必使用你能够找到的最近的股票价格，并注明该价格的获取日期。**同时，为每个推荐给出简短的入选理由（**每个理由请控制在200字以内**）。
4. **相关资讯链接**：提供你所分析的市场的相关财经资讯链接，包括美股、港股和沪深股市。

你**不允许**在JSON结构的前后添加任何额外文本、解释或免责声明。请将所有分析结果以**严格的JSON格式**返回，确保可直接解析。JSON对象的结构如下：
"""
    
    prompt_text = f"{prompt_prefix}{json.dumps(json_schema, indent=4, ensure_ascii=False)}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "tools": [{"google_search": {}}]
    }
    
    headers = { "Content-Type": "application/json" }
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={GEMINI_API_KEY}"
    
    print("开始调用 Gemini API...")
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result_json = response.json()
        raw_text = result_json['candidates'][0]['content']['parts'][0]['text']
        print("成功从 Gemini API 获取响应。")
        print(f"原始响应文本: {raw_text}")
        return raw_text
    except Exception as e:
        error_msg = f"Gemini API 调用失败: {e}"
        print(error_msg)
        return None

def _parse_gemini_response(raw_text):
    """从原始文本中解析JSON数据并添加日期前缀"""
    if not raw_text:
        return None
    
    # --- 优化后的JSON解析逻辑 ---
    # 找到第一个 '{' 和最后一个 '}'，并提取中间的字符串
    try:
        json_start_index = raw_text.find('{')
        if json_start_index == -1:
            raise ValueError("无法在文本中找到JSON的起始字符 '{'")
            
        # 寻找最外层的最后一个 '}'
        json_end_index = raw_text.rfind('}')
        if json_end_index == -1 or json_end_index < json_start_index:
            raise ValueError("无法在文本中找到JSON的结束字符 '}'")
            
        json_text = raw_text[json_start_index : json_end_index + 1]
        
        # 验证提取的文本是否为有效的 JSON
        analysis_data = json.loads(json_text)
        print("成功解析 JSON 数据。")

        # 添加日期信息前缀
        end_date = datetime.now()
        start_date = end_date - timedelta(days=6)
        date_prefix = f"过去一周（{start_date.strftime('%Y年%m月%d日')}-{end_date.strftime('%d日')}）：\n\n"
        
        analysis_data['overallSummary'] = date_prefix + analysis_data['overallSummary']
        
        return analysis_data
    except (json.JSONDecodeError, ValueError) as e:
        error_msg = f"解析 JSON 失败: {e}\n\n原始文本:\n{raw_text}"
        print(error_msg)
        send_email_notification(GMAIL_RECIPIENT_EMAILS, "理财分析任务失败", error_msg)
        return None

# --- Storage and Notification Functions ---
def _save_to_firestore(data):
    """Save data to Firestore database"""
    if not db:
        print("Firestore Admin SDK not initialized, skipping write.")
        return False
    try:
        doc_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('finance_reports').document('latest')
        doc_ref.set(data)
        print("Successfully wrote data to Firestore.")
        return True
    except Exception as e:
        print(f"Failed to write to Firestore: {e}")
        return False

def _save_to_notion(data):
    """Save data to Notion database"""
    try:
        title = f"每周金融分析报告 - {datetime.now().strftime('%Y-%m-%d')}"
        link = "N/A"
        
        def process_stocks_for_notion(stocks_list, max_len=1900):
            """Convert stock recommendation list to a JSON string and ensure it doesn't exceed max_len"""
            json_str = json.dumps(stocks_list, indent=2, ensure_ascii=False)
            if len(json_str) > max_len:
                print(f"Stock list JSON string is too long ({len(json_str)}), truncating...")
                truncated_str = json_str[:max_len-5] + '...' + json_str[-2:]
                return truncated_str
            return json_str

        us_stocks_notion = process_stocks_for_notion(data['usTop10Stocks'])
        hk_stocks_notion = process_stocks_for_notion(data['hkTop10Stocks'])
        cn_stocks_notion = process_stocks_for_notion(data['cnTop10Stocks'])

        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": title}}]},
                "URL": {"url": link},
                "OverallSentiment": {"select": {"name": data['overallSentiment']}},
                "OverallSummary": {"rich_text": [{"text": {"content": data['overallSummary']}}]},
                "DailyCommentary": {"rich_text": [{"text": {"content": data['dailyCommentary']}}]},
                "usTop10Stocks": {"rich_text": [{"text": {"content": us_stocks_notion}}]},
                "hkTop10Stocks": {"rich_text": [{"text": {"content": hk_stocks_notion}}]},
                "cnTop10Stocks": {"rich_text": [{"text": {"content": cn_stocks_notion}}]},
                "CrawledDate": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        print(f"Successfully wrote to Notion: {title}")
        return True
    except Exception as e:
        print(f"Failed to write to Notion: {e}")
        return False

def _generate_html_email_body(data):
    """
    Generates a modern, clean HTML body for the weekly financial report email.
    
    This function has been completely refactored to improve aesthetics and clarity.
    Key changes:
    - Overall layout is now a clean, centered card with a subtle shadow.
    - Uses a consistent color palette and typography for professionalism.
    - Stock recommendations are presented in three distinct, styled tables for better readability.
    - Tables feature a header row and alternating row colors (zebra stripes) to make scanning easier.
    """
    def get_change_color(change):
        if isinstance(change, (int, float)):
            if change > 0: return "#16a34a"  # Green
            elif change < 0: return "#dc2626"  # Red
        return "#4b5563"  # Gray

    def format_stocks_table_html(stocks, market_name):
        html = f"""
        <div style="margin-bottom: 2rem;">
            <h3 style="font-size: 1.25rem; font-weight: bold; color: #111827; margin-bottom: 1rem; text-align: center;">{market_name}</h3>
            <div style="overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: 8px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);">
                <table style="width:100%; min-width: 600px; border-collapse: collapse; font-size: 0.875rem; color: #374151; table-layout: fixed;">
                    <thead>
                        <tr style="background-color:#e5e7eb; color:#4b5563; font-weight: bold;">
                            <th style="padding: 1rem 0.75rem; text-align: left;">代码/公司</th>
                            <th style="padding: 1rem 0.75rem; text-align: left;">价格/市值</th>
                            <th style="padding: 1rem 0.75rem; text-align: left;">主要比率</th>
                            <th style="padding: 1rem 0.75rem; text-align: left;">涨跌</th>
                            <th style="padding: 1rem 0.75rem; text-align: left;">推荐理由</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for i, s in enumerate(stocks):
            row_bg_color = "#f9fafb" if i % 2 == 0 else "#ffffff"
            weekly_change = s.get('weeklyChange', 'N/A')
            monthly_change = s.get('monthlyChange', 'N/A')
            weekly_change_str = f'{weekly_change}%' if isinstance(weekly_change, (int, float)) else weekly_change
            monthly_change_str = f'{monthly_change}%' if isinstance(monthly_change, (int, float)) else monthly_change
            
            html += f"""
                        <tr style="background-color:{row_bg_color};">
                            <td style="padding: 0.75rem; border-top: 1px solid #e5e7eb; font-weight: 600;">{s['stockCode']}<br>{s['companyName']}</td>
                            <td style="padding: 0.75rem; border-top: 1px solid #e5e7eb;">价格: {s.get('price', 'N/A')}<br>市值: {s.get('marketCap', 'N/A')}</td>
                            <td style="padding: 0.75rem; border-top: 1px solid #e5e7eb;">
                                PE: {s.get('peRatio', 'N/A')}<br>
                                PB: {s.get('pbRatio', 'N/A')}<br>
                                PS: {s.get('psRatio', 'N/A')}<br>
                                ROE: {s.get('roeRatio', 'N/A')}
                            </td>
                            <td style="padding: 0.75rem; border-top: 1px solid #e5e7eb;">
                                周: <span style="color:{get_change_color(s.get('weeklyChange'))}; font-weight:600;">{weekly_change_str}</span><br>
                                月: <span style="color:{get_change_color(s.get('monthlyChange'))}; font-weight:600;">{monthly_change_str}</span>
                            </td>
                            <td style="padding: 0.75rem; border-top: 1px solid #e5e7eb; vertical-align: top;">{s['reason']}</td>
                        </tr>
            """
        html += '</tbody></table></div></div>'
        return html

    daily_commentary_html = data['dailyCommentary'].replace('\n', '<br>')
    us_stocks_html = format_stocks_table_html(data['usTop10Stocks'], '美股 (US)')
    hk_stocks_html = format_stocks_table_html(data['hkTop10Stocks'], '港股 (HK)')
    cn_stocks_html = format_stocks_table_html(data['cnTop10Stocks'], '沪深股市 (CN)')

    news_links_html = ""
    if data.get('relatedNewsLinks'):
        news_links_html = """
        <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 2rem;">
            <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">相关财经资讯</h2>
            <ul style="list-style-type: none; padding: 0; margin-top: 1rem;">
        """
        for link in data['relatedNewsLinks']:
            news_links_html += f"""
                <li style="margin-bottom: 0.5rem;"><a href="{link.get('url', '#')}" style="color: #2563eb; text-decoration: none;">{link.get('title', '无标题链接')}</a></li>
            """
        news_links_html += '</ul></div>'
    
    sentiment_color_map = {'利好': '#dcfce7', '利空': '#fee2e2', '中性': '#fef9c3'}
    sentiment_text_color_map = {'利好': '#16a34a', '利空': '#dc2626', '中性': '#ca8a04'}
    sentiment_bg = sentiment_color_map.get(data['overallSentiment'], '#f3f4f6')
    sentiment_text = sentiment_text_color_map.get(data['overallSentiment'], '#374151')

    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
        </style>
    </head>
    <body style="font-family: 'Inter', sans-serif; background-color: #f1f5f9; padding: 2rem 1rem;">
        <div style="max-width: 800px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; box-shadow: 0 6px 12px rgba(0, 0, 0, 0.08); padding: 2rem;">
            <h1 style="text-align: center; font-size: 2.5rem; font-weight: 800; color: #111827; margin-bottom: 0.5rem; line-height: 1.2;">每周金融分析报告</h1>
            <p style="text-align: center; font-size: 1.125rem; color: #4b5563; margin-bottom: 2rem;">由 Gemini AI 自动生成</p>

            <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); margin-bottom: 2rem;">
                <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827; text-align: center;">整体市场情绪</h2>
                <div style="font-size: 2.25rem; font-weight: bold; border-radius: 8px; padding: 1rem; margin-top: 1rem; text-align: center; background-color: {sentiment_bg}; color: {sentiment_text};">
                    {data['overallSentiment']}
                </div>
            </div>

            <div style="display: grid; gap: 2rem; margin-bottom: 2rem;">
                <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                    <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">整体摘要</h2>
                    <p style="margin-top: 1rem; color: #374151; line-height: 1.6;">{data['overallSummary']}</p>
                </div>
                <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                    <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827;">每周点评</h2>
                    <p style="margin-top: 1rem; color: #374151; line-height: 1.6;">{daily_commentary_html}</p>
                </div>
            </div>
            
            {news_links_html}

            <div style="background-color: #f9fafb; padding: 1.5rem; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                <h2 style="font-size: 1.5rem; font-weight: bold; color: #111827; text-align: center;">中长线投资推荐</h2>
                <div style="margin-top: 1.5rem;">
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

# --- Main execution function ---
def main():
    """Main function: execute all tasks sequentially"""
    raw_text = _get_gemini_analysis()
    if not raw_text:
        return

    analysis_data = _parse_gemini_response(raw_text)
    if not analysis_data:
        return

    # Attempt to write data to Firestore and Notion
    # As per the user's request, the outcome of Firestore write will be ignored for the email sending logic
    firestore_success = _save_to_firestore(analysis_data)
    notion_success = _save_to_notion(analysis_data)

    if notion_success:
        subject = f"【理财分析】每周报告 - {datetime.now().strftime('%Y-%m-%d')}"
        email_body = _generate_html_email_body(analysis_data)
        send_email_notification(GMAIL_RECIPIENT_EMAILS, subject, email_body, is_html=True)
    else:
        # If Notion write failed, send a specific failure notification
        error_msg = "部分任务失败：Notion写入失败。"
        send_email_notification(GMAIL_RECIPIENT_EMAILS, "理财分析任务部分失败", error_msg)

if __name__ == "__main__":
    main()
