import os
import httpx
from langchain_openai import ChatOpenAI  # 注意这里换成了 OpenAI
from langchain_core.messages import HumanMessage

# 配置信息
API_KEY = "sk-x8CAMqqLBLanjLsrDfcIJE3qTRUUwgsKP7s6f6XHOysl8M4c"
# 注意：OpenAI 兼容接口通常需要以 /v1 结尾
BASE_URL = "https://b4u.qzz.io/v1" 

def get_claude_llm():
    # 既然你在 RooCode 里能成功，说明这种 OpenAI 兼容模式更稳定。
    # 我们使用 ChatOpenAI 来调用它。
    
    llm = ChatOpenAI(
        model="claude-4.5-sonnet", # 对应截图中的模型名
        openai_api_key=API_KEY,
        openai_api_base=BASE_URL,
        # 如果需要代理，取消下面这行的注释，并填入你的代理地址
        # http_client=httpx.Client(proxies="http://127.0.0.1:7890"), 
        temperature=0.7,
        max_tokens=4096
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