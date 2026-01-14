import os
import httpx
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

API_KEY = "sk-x8CAMqqLBLanjLsrDfcIJE3qTRUUwgsKP7s6f6XHOysl8M4c"
BASE_URL = "https://b4u.qzz.io/v1" 

def get_claude_llm():
    
    llm = ChatOpenAI(
        model="claude-4.5-sonnet",
        openai_api_key=API_KEY,
        openai_api_base=BASE_URL,
        # http_client=httpx.Client(proxies="http://127.0.0.1:7890"), 
        temperature=0.1
    )
    return llm

if __name__ == "__main__":
    try:
        # 测试调用
        print(f"正在通过 OpenAI 兼容协议连接到 {BASE_URL} ...")
        messages = [HumanMessage(content="Hello! active?")]
        
        # Invoke
        response = get_claude_llm().invoke(messages)
        
        print("\n--- 调用成功 ---")
        print(response.content)
        
    except Exception as e:
        print(f"\n--- 调用失败 ---")
        print(f"错误信息: {e}")