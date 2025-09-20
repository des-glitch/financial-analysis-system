import os
import requests
from bs4 import BeautifulSoup
from notion_client import Client
import base64
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import json

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
def analyze_content(content):
    """
    使用Gemini API对文章内容进行总结、情绪和行情分析
    """
    prompt_text = f"""
    对以下财经新闻进行分析：

    {content}

    请输出JSON格式的结果，包含 'summary', 'sentiment' (从 '利好', '利空', '中性' 中选择), 和 'market_analysis'。
    例如: {{'summary': '...', 'sentiment': '...', 'market_analysis': '...'}}
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={GEMINI_API_KEY}"
    
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # 检查HTTP错误
        
        result_json = response.json()
        
        # 解析返回的JSON字符串
        analysis_data = json.loads(result_json['candidates'][0]['content']['parts'][0]['text'])
        
        return analysis_data['summary'], analysis_data['sentiment'], analysis_data['market_analysis']
    except Exception as e:
        print(f"调用Gemini API失败: {e}")
        return "AI总结失败", "中性", "AI分析失败"

# --- 数据爬取与分析主函数 ---
def fetch_and_analyze_news():
    """主函数：爬取、分析、存储并推送"""
    # 这是一个示例爬虫，请根据你的需求替换为实际的爬虫代码
    # 获取新闻标题、链接和全文内容
    articles_to_process = [
        {"title": "美股市场强劲反弹，科技股领涨", "link": "https://example.com/news1", "content": "这是文章的全文内容，包含了关于美股市场强劲反弹、科技股领涨的具体信息。"}
    ]
    
    try:
        for article in articles_to_process:
            title = article["title"]
            link = article["link"]
            content = article["content"]
            
            # AI分析
            summary, sentiment, market_analysis = analyze_content(content)
            
            # 写入Notion数据库
            write_to_notion(title, link, summary, sentiment, market_analysis)
            
            # 发送邮件通知
            email_subject = f"【理财分析】新文章：{title}"
            email_body = f"AI总结：{summary}\n\n情绪分析：{sentiment}\n行情判断：{market_analysis}\n\n原文链接：{link}"
            send_email_notification(GMAIL_RECIPIENT_EMAIL, email_subject, email_body)
            
    except Exception as e:
        print(f"爬取或分析失败：{e}")
        send_email_notification(GMAIL_RECIPIENT_EMAIL, "理财分析任务失败", f"爬取任务失败：{e}")

# --- Notion数据库写入函数 ---
def write_to_notion(title, url, summary, sentiment, analysis):
    """将数据写入Notion数据库"""
    try:
        notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": title}}]},
                "URL": {"url": url},
                "Summary": {"rich_text": [{"text": {"content": summary}}]},
                "Sentiment": {"select": {"name": sentiment}},
                "MarketAnalysis": {"rich_text": [{"text": {"content": analysis}}]},
                "CrawledDate": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        print(f"成功写入Notion：{title}")
    except Exception as e:
        print(f"写入Notion失败：{e}")

if __name__ == "__main__":
    fetch_and_analyze_news()

