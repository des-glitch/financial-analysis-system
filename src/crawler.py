# -*- coding: utf-8 -*-

"""
这个脚本是一个理财分析生成工具，主要功能如下：

1.  **AI 分析**：调用 Gemini API，获取对全球主要市场（美股、港股、A股）的宏观分析和个股推荐。
2.  **数据抓取**：使用 Alpha Vantage 和 Tushare 接口获取推荐股票的实时价格、市值、市盈率等关键数据。
3.  **多平台同步**：将生成的金融周报同步到 Notion 和 Firestore 数据库。
4.  **邮件通知**：将格式化好的周报以 HTML 邮件的形式发送给指定的收件人。

**运行环境依赖**：
请确保在运行此脚本之前，已安装所有必需的 Python 库。您可以通过以下命令安装：

pip install tushare requests notion-client google-api-python-client google-auth-oauthlib firebase-admin

**环境变量配置**：
本脚本依赖多个环境变量来访问 API 和服务。请确保已在您的运行环境中（如 GitHub Actions Secrets）配置以下变量：
-   NOTION_TOKEN
-   NOTION_DATABASE_ID
-   GMAIL_CLIENT_ID
-   GMAIL_CLIENT_SECRET
-   GMAIL_REFRESH_TOKEN
-   GMAIL_RECIPIENT_EMAILS
-   FIREBASE_CONFIG_JSON
-   ALPHA_VANTAGE_API_KEY
-   TUSHARE_API_KEY
-   __app_id

**注意**：脚本在生成香港股票链接时，已修复腾讯等公司代码的补零问题，确保链接正确。
"""

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
import tushare as ts

# --- Configuration ---
# Get environment variables from GitHub Actions Secrets
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN")

gmail_emails_str = os.environ.get("GMAIL_RECIPIENT_EMAILS") or os.environ.get("GMAIL_RECIPIENT_EMAIL")
if gmail_emails_str:
    GMAIL_RECIPIENT_EMAILS = [email.strip() for email in gmail_emails_str.split(',')]
else:
    GMAIL_RECIPIENT_EMAILS = []

# Get Firebase config from environment variables
FIREBASE_CONFIG_JSON = os.environ.get("FIREBASE_CONFIG_JSON")
APP_ID = os.environ.get("__app_id")

# --- Gemini API Configuration ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# --- Stock API Configuration ---
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
TUSHARE_API_KEY = os.environ.get("TUSHARE_API_KEY")

# --- Initialize clients ---
notion = Client(auth=NOTION_TOKEN)

# Initialize Firebase Admin SDK
db = None
if FIREBASE_CONFIG_JSON:
    try:
        cred = credentials.Certificate(json.loads(FIREBASE_CONFIG_JSON))
        initialize_app(cred)
        db = firestore.client()
        print("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Firebase Admin SDK. Check FIREBASE_CONFIG_JSON: {e}")
else:
    print("FIREBASE_CONFIG_JSON environment variable not found. Firebase Admin SDK not initialized.")

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
            message = create_message("me", to.strip(), subject, message_text, "html" if is_html else "plain")
            service.users().messages().send(userId="me", body=message).execute()
            print(f"Successfully sent email to: {to.strip()}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# --- Core logic function: Call AI and parse data ---
def _get_gemini_analysis():
    """
    Call the Gemini API and return the raw response text.
    The AI will now only provide stock codes and reasons, not real-time data.
    """
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
                "reason": "..."
            }
        ],
        "hkTop10Stocks": [
            {
                "stockCode": "700.HK",
                "companyName": "腾讯控股",
                "reason": "..."
            }
        ],
        "cnTop10Stocks": [
            {
                "stockCode": "600519.SH",
                "companyName": "贵州茅台",
                "reason": "..."
            }
        ]
    }
    
    prompt_prefix = """
你是一名资深金融分析师。你必须严格根据可联网搜索到的过去一周（七天）的财经新闻和市场数据进行分析。

请完成以下分析任务：
1. **整体市场情绪和摘要**：给出对整体市场情绪的判断（利好、利空或中性），并提供一份整体行情摘要。
2. **每周点评与预判**：给出对美股、港股和大陆股市的专业点评和对后续走势的预判。请将此部分内容格式化为清晰的文本，用“美股市场点评：”等标题区分。
3. **中长线投资推荐**：
   - 选出美股、港股和中国沪深股市各10个值得中长线买入的股票。
   - **核心要求**：为每个推荐提供对应的公司中文名称、股票代码以及简短的入选理由（每个理由请控制在200字以内）。
   - **重要提示**：请不要在你的分析中提供任何股票价格、市值或涨跌幅数据。这些数据将由另一个独立的程序模块获取。
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
    
    try:
        json_start_index = raw_text.find('{')
        if json_start_index == -1:
            raise ValueError("无法在文本中找到JSON的起始字符 '{'")
            
        json_end_index = raw_text.rfind('}')
        if json_end_index == -1 or json_end_index < json_start_index:
            raise ValueError("无法在文本中找到JSON的结束字符 '}'")
            
        json_text = raw_text[json_start_index : json_end_index + 1]
        
        analysis_data = json.loads(json_text)
        print("成功解析 JSON 数据。")

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

def _get_us_stock_data(stock_code):
    """
    Get US stock data (price, weekly change, and fundamentals) from Alpha Vantage API.
    """
    if not ALPHA_VANTAGE_API_KEY:
        print("ALPHA_VANTAGE_API_KEY environment variable not set. Skipping US stock API call.")
        return None

    # Get real-time data
    price_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock_code}&apikey={ALPHA_VANTAGE_API_KEY}'
    try:
        r = requests.get(price_url)
        r.raise_for_status()
        data = r.json().get('Global Quote', {})
        if data:
            price = data.get('05. price', 'N/A')
            weekly_change = data.get('10. change percent', 'N/A').strip('%')
            print(f"获取 {stock_code} 最新报价数据成功: 价格={price}, 涨幅={weekly_change}")
            
            # Wait to avoid API limit
            time.sleep(15)

            # Get fundamental data
            overview_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={stock_code}&apikey={ALPHA_VANTAGE_API_KEY}'
            r_overview = requests.get(overview_url)
            r_overview.raise_for_status()
            overview_data = r_overview.json()

            # Wait to avoid API limit
            time.sleep(15)

            market_cap = overview_data.get('MarketCapitalization', 'N/A')
            pe_ratio = overview_data.get('PERatio', 'N/A')
            ps_ratio = overview_data.get('PriceToSalesRatioTTM', 'N/A')
            roe_ratio = overview_data.get('ReturnOnEquityTTM', 'N/A')
            pb_ratio = overview_data.get('PriceToBookRatio', 'N/A')

            if isinstance(market_cap, str) and market_cap.isdigit():
                market_cap_value = int(market_cap)
                if market_cap_value >= 1_000_000_000_000:
                    market_cap = f"{market_cap_value / 1_000_000_000_000:.2f} T"
                elif market_cap_value >= 1_000_000_000:
                    market_cap = f"{market_cap_value / 1_000_000_000:.2f} B"
                else:
                    market_cap = f"{market_cap_value}"

            return {
                "price": f"{price} USD",
                "weeklyChange": float(weekly_change) if weekly_change.replace('.', '', 1).isdigit() else "N/A",
                "marketCap": market_cap,
                "peRatio": pe_ratio,
                "psRatio": ps_ratio,
                "roeRatio": roe_ratio,
                "pbRatio": pb_ratio,
                "sourceLink": f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={stock_code}",
            }
        else:
            print(f"无法从 Alpha Vantage 获取 {stock_code} 报价数据。")
            return None
    except Exception as e:
        print(f"调用 Alpha Vantage API 失败: {e}")
        return None

def _get_cn_hk_stock_data(stock_code):
    """
    Get CN/HK stock data (price and weekly change) from Tushare API.
    """
    if not TUSHARE_API_KEY:
        print("TUSHARE_API_KEY environment variable not set. Skipping CN/HK stock API call.")
        return None
    
    ts.set_token(TUSHARE_API_KEY)
    pro = ts.pro_api()

    # Tushare uses a different code format, e.g., '600519.SH' -> '600519.SH'
    tushare_code = stock_code.upper()
    
    # Check for HK stock and if it needs a leading zero for Yahoo Finance
    yahoo_code = stock_code
    if '.HK' in stock_code.upper():
        numeric_part = stock_code.upper().replace('.HK', '')
        # Yahoo Finance requires a leading zero for 3-digit HK codes like 700.HK -> 0700.HK
        if len(numeric_part) < 4:
            yahoo_code = numeric_part.zfill(4) + '.HK'
    
    try:
        # NOTE: 使用 'daily_basic' API，该接口提供非复权日线行情，以适应Tushare的120积分限制。
        print(f"尝试使用 'daily_basic' API 获取 {stock_code} 的数据...")
        df = pro.daily_basic(ts_code=tushare_code, fields='trade_date,close,pe_ttm,pb,total_mv,change_pct')
        if not df.empty:
            latest_data = df.iloc[0]
            close_price = latest_data['close']
            pe_ratio = latest_data['pe_ttm']
            pb_ratio = latest_data['pb']
            market_cap_billion = latest_data['total_mv'] / 10000.0  # Convert to billion
            weekly_change = latest_data['change_pct']
            
            return {
                "price": f"{close_price} CNY" if '.SH' in tushare_code or '.SZ' in tushare_code else f"{close_price} HKD",
                "weeklyChange": weekly_change,
                "marketCap": f"{market_cap_billion:.2f} B",
                "peRatio": pe_ratio,
                "pbRatio": pb_ratio,
                "sourceLink": f"https://finance.yahoo.com/quote/{yahoo_code}"
            }
        else:
            print(f"无法从 Tushare 获取 {stock_code} 数据。")
            return None
    except Exception as e:
        print(f"调用 Tushare API 失败: {e}")
        return None


def _enrich_stock_data(stocks_list, market_type):
    """
    Enriches stock list with real-time and fundamental data based on market type.
    """
    if not stocks_list:
        return []
    
    enriched_stocks = []
    for i, stock in enumerate(stocks_list):
        stock_code = stock.get('stockCode')

        if market_type == 'us':
            # Handle US stocks
            data = _get_us_stock_data(stock_code)
            if data:
                stock.update(data)
            else:
                stock['price'] = "N/A"
                stock['marketCap'] = "N/A"
                stock['weeklyChange'] = "N/A"
                stock['peRatio'] = "N/A"
                stock['psRatio'] = "N/A"
                stock['roeRatio'] = "N/A"
                stock['pbRatio'] = "N/A"
                stock['sourceLink'] = f"https://finance.yahoo.com/quote/{stock_code}"
        
        elif market_type in ['hk', 'cn']:
            # Handle HK and CN stocks
            data = _get_cn_hk_stock_data(stock_code)
            if data:
                stock.update(data)
            else:
                stock['price'] = "N/A"
                stock['marketCap'] = "N/A"
                stock['weeklyChange'] = "N/A"
                stock['peRatio'] = "N/A"
                stock['pbRatio'] = "N/A"
                stock['sourceLink'] = f"https://finance.yahoo.com/quote/{stock_code}"
                
        enriched_stocks.append(stock)
    
    return enriched_stocks

def _format_stocks_for_notion(stocks):
    """Formats a list of stocks into a compact string for Notion's rich_text property."""
    if not stocks:
        return ""
    
    formatted_list = []
    for i, stock in enumerate(stocks):
        # Create a compact string for each stock
        stock_str = f"[{i+1}. {stock.get('companyName', 'N/A')} ({stock.get('stockCode', 'N/A')}): {stock.get('reason', 'N/A')}]"
        
        # Add basic data if available
        price = stock.get('price')
        if price != 'N/A':
            stock_str += f" | 价格: {price}"
        
        weekly_change = stock.get('weeklyChange')
        if weekly_change != 'N/A':
            stock_str += f" | 周涨幅: {weekly_change}%"
            
        formatted_list.append(stock_str)
        
    # Join the list into a single string, respecting the 2000 character limit
    full_string = "\n\n".join(formatted_list)
    if len(full_string) > 2000:
        return full_string[:1995] + "..."
    
    return full_string

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
    """
    Save the enriched data to Notion database with the specified field structure.
    """
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        print("Notion credentials not set, skipping save to Notion.")
        return False

    try:
        # Format the stock data to fit Notion's rich_text limit
        us_stocks_formatted = _format_stocks_for_notion(data.get('usTop10Stocks', []))
        hk_stocks_formatted = _format_stocks_for_notion(data.get('hkTop10Stocks', []))
        cn_stocks_formatted = _format_stocks_for_notion(data.get('cnTop10Stocks', []))

        new_page_properties = {
            "Title": {
                "title": [
                    {
                        "text": {
                            "content": f"【理财分析】每周理财分析报告 - {datetime.now().strftime('%Y-%m-%d')}"
                        }
                    }
                ]
            },
            "URL": {
                "url": "https://example.com/finance-report"  # Placeholder URL
            },
            "OverallSentiment": {
                "rich_text": [
                    {
                        "text": {
                            "content": data.get('overallSentiment', 'N/A')
                        }
                    }
                ]
            },
            "OverallSummary": {
                "rich_text": [
                    {
                        "text": {
                            "content": data.get('overallSummary', 'N/A')
                        }
                    }
                ]
            },
            "DailyCommentary": {
                "rich_text": [
                    {
                        "text": {
                            "content": data.get('dailyCommentary', 'N/A')
                        }
                    }
                ]
            },
            "usTop10Stocks": {
                "rich_text": [
                    {
                        "text": {
                            "content": us_stocks_formatted
                        }
                    }
                ]
            },
            "hkTop10Stocks": {
                "rich_text": [
                    {
                        "text": {
                            "content": hk_stocks_formatted
                        }
                    }
                ]
            },
            "cnTop10Stocks": {
                "rich_text": [
                    {
                        "text": {
                            "content": cn_stocks_formatted
                        }
                    }
                ]
            },
            "CrawledDate": {
                "date": {
                    "start": datetime.now().isoformat()
                }
            }
        }
        
        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties=new_page_properties
        )

        print("Successfully saved data to Notion.")
        return True
    except Exception as e:
        print(f"Failed to save data to Notion: {e}")
        return False

def _format_html_report(data):
    """
    Format the analysis data into a nice-looking HTML report for email.
    """
    report_date = datetime.now().strftime('%Y年%m月%d日')
    
    # Sanitize commentary before inserting into HTML
    commentary_html = data.get('dailyCommentary', 'N/A').replace('\n', '<br><br>')
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>【理财分析】每周理财分析报告 - {report_date}</title>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f4f7f6;
                color: #333;
            }}
            .container {{
                max-width: 800px;
                margin: 0 auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
                padding: 30px;
            }}
            .header {{
                text-align: center;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 20px;
                margin-bottom: 20px;
            }}
            .header h1 {{
                font-size: 28px;
                color: #1a1a1a;
                margin: 0;
            }}
            .header p {{
                color: #777;
                font-size: 14px;
                margin-top: 5px;
            }}
            .section {{
                margin-bottom: 30px;
            }}
            .section-title {{
                font-size: 22px;
                color: #2c3e50;
                border-left: 4px solid #3498db;
                padding-left: 10px;
                margin-bottom: 15px;
            }}
            .content p {{
                line-height: 1.8;
                font-size: 16px;
            }}
            .stock-list {{
                list-style-type: none;
                padding: 0;
            }}
            .stock-item {{
                background-color: #f9f9f9;
                border: 1px solid #e0e0e0;
                border-radius: 8px;
                padding: 15px;
                margin-bottom: 15px;
                transition: transform 0.2s;
            }}
            .stock-item:hover {{
                transform: translateY(-3px);
            }}
            .stock-item h4 {{
                margin: 0 0 5px 0;
                color: #2980b9;
                font-size: 18px;
            }}
            .stock-item p {{
                margin: 0;
                font-size: 14px;
                line-height: 1.6;
            }}
            .stock-reason {{
                margin-top: 10px;
                color: #555;
            }}
            .link-section {{
                margin-top: 20px;
            }}
            .link-section a {{
                color: #3498db;
                text-decoration: none;
            }}
            .link-section a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>【理财分析】每周理财分析报告</h1>
                <p>生成日期: {report_date}</p>
                <p>由 Gemini AI 提供分析，数据由 Alpha Vantage 和 Tushare 提供</p>
            </div>

            <div class="section">
                <h2 class="section-title">核心分析</h2>
                <div class="content">
                    <p><strong>整体市场情绪:</strong> {data.get('overallSentiment', 'N/A')}</p>
                    <p>{data.get('overallSummary', 'N/A')}</p>
                </div>
            </div>

            <div class="section">
                <h2 class="section-title">每周点评与预判</h2>
                <div class="content">
                    {commentary_html}
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">中长线投资推荐</h2>
                <div class="content">
                    <h3>美股 Top 10</h3>
                    <ul class="stock-list">
    """
    
    # Helper to add stock items
    def add_stocks_to_html(stock_list, market_name):
        nonlocal html_content
        
        if stock_list:
            for stock in stock_list:
                stock_info_text = f"""
                <li class="stock-item">
                    <h4>{stock.get('companyName', 'N/A')} ({stock.get('stockCode', 'N/A')})</h4>
                    <p class="stock-reason"><strong>入选理由:</strong> {stock.get('reason', 'N/A')}</p>
                    <p>
                        <strong>最新价格:</strong> {stock.get('price', 'N/A')} | 
                        <strong>市值:</strong> {stock.get('marketCap', 'N/A')}<br>
                        <strong>周涨幅:</strong> {stock.get('weeklyChange', 'N/A')}%
                    </p>
                """
                if market_name == "美股":
                    stock_info_text += f"""
                    <p>
                        <strong>市盈率 (PE):</strong> {stock.get('peRatio', 'N/A')} | 
                        <strong>市销率 (PS):</strong> {stock.get('psRatio', 'N/A')}<br>
                        <strong>净资产收益率 (ROE)**:</strong> {stock.get('roeRatio', 'N/A')}%
                    </p>
                    """
                elif market_name in ["港股", "A股"]:
                    stock_info_text += f"""
                    <p>
                        <strong>市盈率 (PE):</strong> {stock.get('peRatio', 'N/A')} | 
                        <strong>市净率 (PB)**:</strong> {stock.get('pbRatio', 'N/A')}
                    </p>
                    """
                
                stock_info_text += f"""
                    <p class="link-section"><a href="{stock.get('sourceLink', '#')}">查看更多数据</a></p>
                </li>
                """
                html_content += stock_info_text
        else:
            html_content += f"<li><p>暂无{market_name}推荐。</p></li>"

    add_stocks_to_html(data.get('usTop10Stocks'), '美股')
    html_content += "</ul><h3>港股 Top 10</h3><ul class='stock-list'>"
    add_stocks_to_html(data.get('hkTop10Stocks'), '港股')
    html_content += "</ul><h3>A股 Top 10</h3><ul class='stock-list'>"
    add_stocks_to_html(data.get('cnTop10Stocks'), 'A股')
    html_content += "</ul></div></div>"

    html_content += """
            <div class="section">
                <h2 class="section-title">相关资讯</h2>
                <ul class="stock-list">
    """
    
    for link in data.get('relatedNewsLinks', []):
        html_content += f"""
        <li class="stock-item">
            <p><strong>{link.get('title', 'N/A')}</strong></p>
            <p class="link-section"><a href="{link.get('url', '#')}">{link.get('url', '#')}</a></p>
        </li>
        """

    html_content += """
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    return html_content

def main():
    """Main function to orchestrate the entire process."""
    print("开始生成金融周报...")

    # 1. Get analysis from Gemini
    raw_gemini_text = _get_gemini_analysis()
    if not raw_gemini_text:
        print("未能获取 Gemini 分析，任务终止。")
        return

    # 2. Parse and enrich the data
    analysis_data = _parse_gemini_response(raw_gemini_text)
    if not analysis_data:
        print("未能解析 Gemini 响应，任务终止。")
        return

    # Enrich stock data
    us_stocks = _enrich_stock_data(analysis_data.get('usTop10Stocks', []), 'us')
    hk_stocks = _enrich_stock_data(analysis_data.get('hkTop10Stocks', []), 'hk')
    cn_stocks = _enrich_stock_data(analysis_data.get('cnTop10Stocks', []), 'cn')
    
    analysis_data['usTop10Stocks'] = us_stocks
    analysis_data['hkTop10Stocks'] = hk_stocks
    analysis_data['cnTop10Stocks'] = cn_stocks
    
    # 3. Save data to Notion and Firestore
    _save_to_notion(analysis_data)
    _save_to_firestore(analysis_data)

    # 4. Send email notification
    html_report = _format_html_report(analysis_data)
    subject = f"【理财分析】每周理财分析报告 - {datetime.now().strftime('%Y-%m-%d')}"
    send_email_notification(GMAIL_RECIPIENT_EMAILS, subject, html_report, is_html=True)
    
    print("金融周报生成任务完成。")

if __name__ == "__main__":
    main()
