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

# --- 配置部分 ---
# 从GitHub Actions Secrets获取环境变量
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")

GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN")
GMAIL_RECIPIENT_EMAIL = os.environ.get("GMAIL_RECIPIENT_EMAIL")

# --- Gemini API配置 ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# --- 初始化客户端 ---
# 初始化Notion客户端
notion = Client(auth=NOTION_TOKEN)

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

def create_message(sender, to, subject, message_text):
    """创建一个MIMEText消息对象"""
    message = MIMEText(message_text)
    message["to"] = to
    message["from"] = sender
    message["subject"] = subject
    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_email_notification(to, subject, message_text):
    """使用Gmail API发送邮件"""
    try:
        service = get_gmail_service()
        # 'me' 指的是经过身份验证的用户邮箱地址
        message = create_message("me", to, subject, message_text)
        service.users().messages().send(userId="me", body=message).execute()
        print(f"成功发送邮件至: {to}")
    except Exception as e:
        print(f"发送邮件失败: {e}")

# --- AI分析部分 (使用Gemini API) ---
def fetch_and_analyze_news():
    """
    使用Gemini API同时完成新闻爬取和分析任务
    """
    prompt_text = f"""
    你是一名资深金融分析师，拥有对美股、港股和中国沪深股市的深度分析能力。请根据最新的财经新闻和市场数据，完成以下分析任务。

    首先，从以下主流财经媒体和通讯社中获取最新的市场动态、政策变化和公司财报新闻：
    - 美国：路透社 (Reuters)、华尔街日报 (The Wall Street Journal)、彭博社 (Bloomberg)
    - 香港：路透社中文网、香港经济日报
    - 大陆：新浪财经、东方财富网、证券时报

    在获取了这些信息后，请完成以下分析：

    1. **整体市场情绪和摘要：** 给出对整体市场情绪的判断（利好、利空或中性），并提供一份整体行情摘要。
    2. **每日点评与预判：** 针对前一日的美股、港股和大陆股市，给出专业的点评和对后续走势的预判。
    3. **中长线投资推荐：** 选出美股、港股和中国沪深股市各10个值得中长线买入的股票代码（不限于具体公司、指数或ETF），并为每个推荐给出简短的入选理由。

    请将所有分析结果以严格的JSON格式返回，确保可直接解析。JSON对象的结构如下：

    {{
      "overallSentiment": "利好",
      "overallSummary": "...",
      "dailyCommentary": "...",
      "usTop10Stocks": [
        {{
          "stockCode": "AAPL",
          "reason": "..."
        }},
        ...
      ],
      "hkTop10Stocks": [
        {{
          "stockCode": "700.HK",
          "reason": "..."
        }},
        ...
      ],
      "cnTop10Stocks": [
        {{
          "stockCode": "600519.SH",
          "reason": "..."
        }},
        ...
      ]
    }}
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "responseMimeType": "application/json"
        },
        "tools": [{"google_search": {}}] # 启用Google搜索工具
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={GEMINI_API_KEY}"
    
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # 检查HTTP错误
        
        result_json = response.json()
        
        analysis_data = json.loads(result_json['candidates'][0]['content']['parts'][0]['text'])
        
        # 写入Notion数据库
        title = f"综合金融分析报告 - {datetime.now().strftime('%Y-%m-%d')}"
        link = "N/A" # 综合报告没有单一链接
        
        write_to_notion(title, link, analysis_data['overallSentiment'], analysis_data['overallSummary'], analysis_data['dailyCommentary'],
                        analysis_data['usTop10Stocks'], analysis_data['hkTop10Stocks'], analysis_data['cnTop10Stocks'])
        
        # 发送邮件通知
        email_subject = f"【理财分析】综合报告 - {datetime.now().strftime('%Y-%m-%d')}"
        email_body = (
            f"整体情绪：{analysis_data['overallSentiment']}\n\n"
            f"整体摘要：{analysis_data['overallSummary']}\n\n"
            f"每日点评：{analysis_data['dailyCommentary']}\n\n"
            f"美股中长线推荐：{json.dumps(analysis_data['usTop10Stocks'], indent=2)}\n\n"
            f"港股中长线推荐：{json.dumps(analysis_data['hkTop10Stocks'], indent=2)}\n\n"
            f"沪深股市中长线推荐：{json.dumps(analysis_data['cnTop10Stocks'], indent=2)}\n\n"
            f"原文链接：{link}"
        )
        send_email_notification(GMAIL_RECIPIENT_EMAIL, email_subject, email_body)

    except Exception as e:
        print(f"Gemini调用或分析失败: {e}")
        send_email_notification(GMAIL_RECIPIENT_EMAIL, "理财分析任务失败", f"Gemini调用或分析失败：{e}")


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
                "usTop10Stocks": {"rich_text": [{"text": {"content": json.dumps(usTop10Stocks)}}]},
                "hkTop10Stocks": {"rich_text": [{"text": {"content": json.dumps(hkTop10Stocks)}}]},
                "cnTop10Stocks": {"rich_text": [{"text": {"content": json.dumps(cnTop10Stocks)}}]},
                "CrawledDate": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        print(f"成功写入Notion：{title}")
    except Exception as e:
        print(f"写入Notion失败：{e}")

if __name__ == "__main__":
    fetch_and_analyze_news()
