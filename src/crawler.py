# -*- coding: utf-8 -*-

"""
这个脚本是一个金融周报生成工具，主要功能如下：

1.  **AI 分析**：调用 Gemini API，获取对全球主要市场（美股、港股、A股）的宏观分析和个股推荐，并生成**定制化的投资组合方案**。
2.  **数据抓取**：使用 Alpha Vantage 和 Tushare 接口获取推荐股票的实时价格、市值、市盈率等关键数据。
3.  **多平台同步**：将生成的金融周报同步到 Notion 和 Firestore 数据库。
4.  **邮件通知**：将格式化好的周报以 HTML 邮件的形式发送给指定的收件人。

**运行环境依赖**：
请确保在运行此脚本之前，已安装所有必需的 Python 库。您可以通过以下命令安装：

pip install tushare requests notion-client sendgrid firebase-admin

**环境变量配置**：
本脚本依赖多个环境变量来访问 API 和服务。请确保已在您的运行环境中（如 GitHub Actions Secrets）配置以下变量：
-   NOTION_TOKEN
-   NOTION_DATABASE_ID
-   SENDGRID_API_KEY        <-- 已添加
-   GMAIL_RECIPIENT_EMAILS  <-- 此变量的第一个邮箱将用作 SendGrid 发件人 (FROM_EMAIL)
-   FIREBASE_CONFIG_JSON
-   ALPHA_VANTAGE_API_KEY
-   TUSHARE_API_KEY
-   __app_id

**注意**：脚本在生成香港股票链接时，已修复腾讯等公司代码的补零问题，确保链接正确。
"""

import os
import requests
from notion_client import Client
from datetime import datetime, timedelta
import json
import time
import re
from firebase_admin import credentials, initialize_app, firestore
import tushare as ts

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# --- Configuration ---
# Get environment variables from GitHub Actions Secrets
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

# --- SendGrid Configuration ---
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")

gmail_emails_str = os.environ.get("GMAIL_RECIPIENT_EMAILS") or os.environ.get("GMAIL_RECIPIENT_EMAIL")

if gmail_emails_str:
    GMAIL_RECIPIENT_EMAILS = [email.strip() for email in gmail_emails_str.split(',')]
    # *** 关键：将列表中的第一个邮箱地址作为 SendGrid 的发件人邮箱 ***
    FROM_EMAIL = GMAIL_RECIPIENT_EMAILS[0] 
else:
    GMAIL_RECIPIENT_EMAILS = []
    FROM_EMAIL = None # 如果没有收件人，则没有发件人

if not FROM_EMAIL:
    print("Warning: 环境变量 'GMAIL_RECIPIENT_EMAILS' 未设置或为空。SendGrid 发件人邮箱 (FROM_EMAIL) 无法确定。")

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
        # Load the configuration string as a dictionary
        firebase_config = json.loads(FIREBASE_CONFIG_JSON)
        
        # Ensure the credential object is correctly formed
        cred = credentials.Certificate(firebase_config)
        
        # Check if the app is already initialized to prevent error
        try:
            initialize_app(cred)
        except ValueError:
            # App is already initialized, usually by a previous script/process
            pass
            
        db = firestore.client()
        print("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Firebase Admin SDK. Check FIREBASE_CONFIG_JSON format: {e}")
else:
    print("FIREBASE_CONFIG_JSON environment variable not found. Firebase Admin SDK not initialized.")

# --- SendGrid Email Function ---
def send_email_notification(to_list, subject, message_text, is_html=False):
    """
    Send an email using the SendGrid API with HTML content.
    """
    # 检查 SENDGRID_API_KEY 是否设置
    if not SENDGRID_API_KEY:
        print("SENDGRID_API_KEY environment variable not set, skipping email sending.")
        return
        
    if not FROM_EMAIL:
        print("FROM_EMAIL (SendGrid Sender) is not set, skipping email sending.")
        return
        
    if not to_list:
        print("No recipient emails specified, skipping email sending.")
        return
        
    try:
        # 使用 SENDGRID_API_KEY 初始化 SendGrid 客户端
        sg = SendGridAPIClient(SENDGRID_API_KEY)

        for to_email in to_list:
            message = Mail(
                from_email=FROM_EMAIL, # 使用动态获取的发件人邮箱
                to_emails=to_email.strip(),
                subject=subject,
                html_content=message_text
            )
            
            response = sg.send(message)
            print(f"Successfully sent email to: {to_email.strip()}, Status Code: {response.status_code}")
            
    except Exception as e:
        print(f"Failed to send email via SendGrid: {e}")

# --- Core logic function: Call AI and parse data ---
def _get_gemini_analysis():
    """
    Call the Gemini API and return the raw response text, including the new investment portfolio.
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
        ],
        # 新增的投资组合方案字段
        "investmentPortfolio": {
            "capital": "300,000 CNY",
            "targetAnnualReturn": ">= 20%",
            "portfolioSummary": "基于市场对科技股和消费复苏的预期，本组合采取长线核心持仓配合短期战术配置的策略，以期达到年化20%以上的目标。组合聚焦香港ETF、港股、A股及跨境基金。",
            "investmentPlan": [
                {
                    "assetName": "恒生科技指数ETF (3033.HK)",
                    "assetType": "港股ETF",
                    "allocationRatio": "25%",
                    "expectedGain": "25% (根据多家投行对香港科技股的估值修正和盈利预期，此ETF具有20%-30%的潜在涨幅，研判依据：...) ",
                    "buyTiming": "在恒生科技指数回落至10日均线附近分批买入，可进行首次投入。",
                    "sellTiming": "除非市场结构性发生变化，否则长线持有。若短期内涨幅超10%，可考虑减仓20%锁定利润。",
                    "holdingStrategy": "长线核心持有"
                },
                {
                    "assetName": "贵州茅台 (600519.SH)",
                    "assetType": "A股股票",
                    "allocationRatio": "20%",
                    "expectedGain": "20% ~ 35% (市场普遍认为消费复苏带来强劲现金流，若回购超预期，上行空间有望触及2000元，潜在涨幅35%，研判依据：...)",
                    "buyTiming": "在市场对消费股悲观时，且股价低于1600元时，分两批买入。",
                    "sellTiming": "当估值显著高于历史中位数（例如PE > 45倍）或公司基本面恶化时，考虑卖出。",
                    "holdingStrategy": "长线核心持有"
                },
            ]
        }
    }
    
    prompt_prefix = """
你是一名资深金融分析师。你必须严格根据可联网搜索到的过去一周（七天）的财经新闻和市场数据进行分析。

请完成以下分析任务：
1. **整体市场情绪和摘要**：给出对整体市场情绪的判断（利好、利空或中性），并提供一份整体行情摘要。
2. **每周点评与预判**：给出对美股、港股和大陆股市的专业点评和对后续走势的预判。
   **注意：此字段 (dailyCommentary) 必须是一个包含所有市场评论的**单行文本字符串**，请使用“美股市场点评：”等标题在文本内区分，不要使用嵌套的JSON对象结构。
3. **中长线投资推荐**：
   - 选出美股、港股和中国沪深股市各10个值得中长线买入的股票。
   - **核心要求**：为每个推荐提供对应的公司中文名称、股票代码以及简短的入选理由（每个理由请控制在200字以内）。
   - **重要提示**：请不要在你的分析中提供任何股票价格、市值或涨跌幅数据。这些数据将由另一个独立的程序模块获取。
4. **定制投资组合方案**：
   - 针对一个拥有 **30万人民币本金** 的投资者，要求投资范围限定为：**香港ETF、港股股票、A股股票和大陆发行的关注美国指数类基金**。
   - 目标是实现**综合年化收益不低于20%**。
   - 请综合当前市场行情和长远利益，给出一个详细的组合方案，并严格按照 investmentPortfolio 字段下的 JSON 结构输出。
   - **投资计划(investmentPlan) 的每个条目必须说明**：资产名称、资产类型、分配比例、**预期股价涨幅（必须基于市场信息和知名投行/机构的研判，并在预测中简要说明研判依据）**、何时适合买入、何时需要卖出，以及是适合长线持有还是短线操作。
5. **相关资讯链接**：提供你所分析的市场的相关财经资讯链接，包括美股、港股和沪深股市。

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
        # print(f"原始响应文本: {raw_text}")
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
        
        # --- FIX for AttributeError: 'dict' object has no attribute 'replace' ---
        # 强制将 dailyCommentary 转换为字符串，以防止 AI 错误地返回字典
        daily_commentary = analysis_data.get('dailyCommentary')
        if isinstance(daily_commentary, dict):
            print("Warning: dailyCommentary returned as dict. Attempting to flatten to string.")
            # 展平字典中的所有字符串值，用两行换行符分隔
            analysis_data['dailyCommentary'] = "\n\n".join(
                f"{k}: {v}" for k, v in daily_commentary.items() if isinstance(v, str)
            )
        # --- END FIX ---

        end_date = datetime.now()
        start_date = end_date - timedelta(days=6)
        date_prefix = f"过去一周（{start_date.strftime('%Y年%m月%d日')}-{end_date.strftime('%d日')}）：\n\n"
        
        analysis_data['overallSummary'] = date_prefix + analysis_data.get('overallSummary', 'N/A')
        
        return analysis_data
    except (json.JSONDecodeError, ValueError) as e:
        error_msg = f"解析 JSON 失败: {e}\n\n原始文本:\n{raw_text}"
        print(error_msg)
        send_email_notification(GMAIL_RECIPIENT_EMAILS, "理财分析任务失败", error_msg)
        return None

def _get_us_stock_data(stock_code):
    """
    Get US stock data (price, weekly change, and fundamentals) from Alpha Vantage API.
    (Function remains the same, used for the main stock recommendation list)
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
    (Function remains the same, used for the main stock recommendation list)
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
        error_message = str(e)
        # 增强：检查是否是权限不足的错误信息
        if "没有接口访问权限" in error_message or "权限的具体详情" in error_message:
             print(f"Tushare API 访问权限受限，跳过数据获取：{error_message}")
             # 返回 None 但不中断进程
             return None
        else:
             print(f"调用 Tushare API 失败: {error_message}")
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
                # Tushare data fetch failed, set placeholders
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
        # Check if weeklyChange is a float or string and format it
        if weekly_change != 'N/A':
            if isinstance(weekly_change, (float, int)):
                stock_str += f" | 周涨幅: {weekly_change:.2f}%"
            else:
                stock_str += f" | 周涨幅: {weekly_change}%"
            
        formatted_list.append(stock_str)
        
    # Join the list into a single string, respecting the 2000 character limit
    full_string = "\n\n".join(formatted_list)
    if len(full_string) > 2000:
        return full_string[:1995] + "..."
    
    return full_string

def _format_portfolio_for_notion(portfolio):
    """Formats the investment portfolio plan into a structured string for Notion's rich_text property."""
    if not portfolio:
        return "N/A"

    summary = f"总本金: {portfolio.get('capital', 'N/A')}\n"
    summary += f"年化目标: {portfolio.get('targetAnnualReturn', 'N/A')}\n\n"
    summary += f"综合摘要:\n{portfolio.get('portfolioSummary', 'N/A')}\n\n"
    
    plan_list = []
    for item in portfolio.get('investmentPlan', []):
        item_str = f"- [{item.get('assetName', 'N/A')} ({item.get('assetType', 'N/A')})]\n"
        item_str += f"  > 比例: {item.get('allocationRatio', 'N/A')}, 预期: {item.get('expectedGain', 'N/A')}\n"
        item_str += f"  > 买入: {item.get('buyTiming', 'N/A')}\n"
        item_str += f"  > 卖出: {item.get('sellTiming', 'N/A')}\n"
        item_str += f"  > 策略: {item.get('holdingStrategy', 'N/A')}\n"
        plan_list.append(item_str)
        
    return summary + "详细方案:\n" + "\n".join(plan_list)

# --- Storage and Notification Functions ---
def _save_to_firestore(data):
    """Save data to Firestore database"""
    if not db:
        print("Firestore Admin SDK not initialized, skipping write.")
        return False
    try:
        doc_ref = db.collection('artifacts').document(APP_ID).collection('public').document('data').collection('finance_reports').document('latest')
        
        # Prepare data for Firestore (remove complex objects if necessary, though the structure is mostly flat now)
        firestore_data = data.copy()
        
        doc_ref.set(firestore_data)
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
        # Format the data for Notion's constraints
        us_stocks_formatted = _format_stocks_for_notion(data.get('usTop10Stocks', []))
        hk_stocks_formatted = _format_stocks_for_notion(data.get('hkTop10Stocks', []))
        cn_stocks_formatted = _format_stocks_for_notion(data.get('cnTop10Stocks', []))
        portfolio_formatted = _format_portfolio_for_notion(data.get('investmentPortfolio', {}))
        daily_commentary_content = data.get('dailyCommentary', 'N/A')

        # Handle the select property for overall sentiment
        overall_sentiment = data.get('overallSentiment', 'N/A')
        sentiment_property = {}
        if overall_sentiment != 'N/A':
            # FIX: Notion API requires 'select' property to be a select object, not rich_text.
            sentiment_property = {
                "select": {
                    "name": overall_sentiment
                }
            }
        
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
            "OverallSentiment": sentiment_property,
            "OverallSummary": {
                "rich_text": [
                    {
                        "text": {
                            "content": data.get('overallSummary', 'N/A')
                        }
                    }
                ]
            },
            # 使用已确保是字符串类型的 dailyCommentary_content
            "DailyCommentary": {
                "rich_text": [
                    {
                        "text": {
                            "content": daily_commentary_content
                        }
                    }
                ]
            },
            # 新增的投资组合字段
            "InvestmentPortfolio": {
                "rich_text": [
                    {
                        "text": {
                            "content": portfolio_formatted
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
    
    # 确保 dailyCommentary 是字符串后再进行 replace
    raw_commentary = data.get('dailyCommentary', 'N/A')
    commentary_html = ""
    if isinstance(raw_commentary, str):
        commentary_html = raw_commentary.replace('\n', '<br><br>')
    else:
        # Fallback in case the defensive parse failed (should not happen now)
        commentary_html = str(raw_commentary) 
    
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
            .stock-table-container {{
                overflow-x: auto;
            }}
            .stock-table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            .stock-table th, .stock-table td {{
                padding: 12px;
                border: 1px solid #e0e0e0;
                text-align: left;
                white-space: nowrap;
                font-size: 13px;
            }}
            .stock-table th {{
                background-color: #f0f0f0;
                font-weight: bold;
                font-size: 14px;
            }}
            .stock-table tr:nth-child(even) {{
                background-color: #fafafa;
            }}
            .stock-table tr:hover {{
                background-color: #f1f1f1;
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
                <h2 class="section-title">定制投资组合方案</h2>
                <div class="content">
                    <h4>方案目标</h4>
                    <p><strong>本金:</strong> {data.get('investmentPortfolio', {}).get('capital', 'N/A')} | 
                       <strong>年化目标:</strong> {data.get('investmentPortfolio', {}).get('targetAnnualReturn', 'N/A')}
                    </p>
                    <h4>综合摘要</h4>
                    <p>{data.get('investmentPortfolio', {}).get('portfolioSummary', 'N/A')}</p>
                </div>
                
                <div class="stock-table-container">
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>资产名称</th>
                                <th>资产类型</th>
                                <th>分配比例</th>
                                <th>预期涨幅（含研判依据）</th>
                                <th>买入建议</th>
                                <th>卖出建议</th>
                                <th>持有策略</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    # Helper to add investment plan items
    investment_plan = data.get('investmentPortfolio', {}).get('investmentPlan', [])
    if investment_plan:
        for item in investment_plan:
            html_content += f"""
                            <tr>
                                <td>{item.get('assetName', 'N/A')}</td>
                                <td>{item.get('assetType', 'N/A')}</td>
                                <td>{item.get('allocationRatio', 'N/A')}</td>
                                <td>{item.get('expectedGain', 'N/A')}</td>
                                <td>{item.get('buyTiming', 'N/A')}</td>
                                <td>{item.get('sellTiming', 'N/A')}</td>
                                <td><strong>{item.get('holdingStrategy', 'N/A')}</strong></td>
                            </tr>
            """
    else:
        html_content += """<tr><td colspan="7">暂无定制投资组合方案。</td></tr>"""

    html_content += "</tbody></table></div></div>"
    
    
    # --- Start of Existing Stock Recommendations (re-formatted to tables) ---
    html_content += """
            <div class="section">
                <h2 class="section-title">中长线投资推荐</h2>
                <div class="stock-table-container">
                    <h3>美股 Top 10</h3>
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>公司</th>
                                <th>代码</th>
                                <th>入选理由</th>
                                <th>最新价格</th>
                                <th>市值</th>
                                <th>周涨幅</th>
                                <th>PE</th>
                                <th>PS</th>
                                <th>ROE</th>
                                <th>详情</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    def add_us_stocks_to_html_table(stock_list):
        nonlocal html_content
        if stock_list:
            for stock in stock_list:
                weekly_change_str = f"{stock.get('weeklyChange', 'N/A')}%" if isinstance(stock.get('weeklyChange'), (float, int)) else stock.get('weeklyChange', 'N/A')
                roe_ratio_str = f"{stock.get('roeRatio', 'N/A')}%" if isinstance(stock.get('roeRatio'), (float, int)) else stock.get('roeRatio', 'N/A')
                html_content += f"""
                            <tr>
                                <td>{stock.get('companyName', 'N/A')}</td>
                                <td>{stock.get('stockCode', 'N/A')}</td>
                                <td>{stock.get('reason', 'N/A')}</td>
                                <td>{stock.get('price', 'N/A')}</td>
                                <td>{stock.get('marketCap', 'N/A')}</td>
                                <td>{weekly_change_str}</td>
                                <td>{stock.get('peRatio', 'N/A')}</td>
                                <td>{stock.get('psRatio', 'N/A')}</td>
                                <td>{roe_ratio_str}</td>
                                <td><a href="{stock.get('sourceLink', '#')}">查看</a></td>
                            </tr>
                """
        else:
            html_content += """<tr><td colspan="10">暂无美股推荐。</td></tr>"""

    def add_other_stocks_to_html_table(stock_list, market_name):
        nonlocal html_content
        if stock_list:
            for stock in stock_list:
                weekly_change_str = f"{stock.get('weeklyChange', 'N/A')}%" if isinstance(stock.get('weeklyChange'), (float, int)) else stock.get('weeklyChange', 'N/A')
                html_content += f"""
                            <tr>
                                <td>{stock.get('companyName', 'N/A')}</td>
                                <td>{stock.get('stockCode', 'N/A')}</td>
                                <td>{stock.get('reason', 'N/A')}</td>
                                <td>{stock.get('price', 'N/A')}</td>
                                <td>{stock.get('marketCap', 'N/A')}</td>
                                <td>{weekly_change_str}</td>
                                <td>{stock.get('peRatio', 'N/A')}</td>
                                <td>{stock.get('pbRatio', 'N/A')}</td>
                                <td><a href="{stock.get('sourceLink', '#')}">查看</a></td>
                            </tr>
                """
        else:
            html_content += f"""<tr><td colspan="9">暂无{market_name}推荐。</td></tr>"""

    add_us_stocks_to_html_table(data.get('usTop10Stocks'))
    html_content += "</tbody></table><h3>港股 Top 10</h3><table class='stock-table'><thead><tr><th>公司</th><th>代码</th><th>入选理由</th><th>最新价格</th><th>市值</th><th>周涨幅</th><th>PE</th><th>PB</th><th>详情</th></tr></thead><tbody>"
    add_other_stocks_to_html_table(data.get('hkTop10Stocks'), '港股')
    html_content += "</tbody></table><h3>A股 Top 10</h3><table class='stock-table'><thead><tr><th>公司</th><th>代码</th><th>入选理由</th><th>最新价格</th><th>市值</th><th>周涨幅</th><th>PE</th><th>PB</th><th>详情</th></tr></thead><tbody>"
    add_other_stocks_to_html_table(data.get('cnTop10Stocks'), 'A股')
    html_content += "</tbody></table></div></div>"
    # --- End of Existing Stock Recommendations ---

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
    # 注意：Tushare 权限错误现在会被捕获并跳过，不会导致整体崩溃
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
