import os
from langchain_anthropic import ChatAnthropic

# 设置环境变量
os.environ["ANTHROPIC_API_KEY"] = "sk-K63B6SmifjmBkevxvq2UFF08bSCN6kpP41qimf8uNze8wRwj"
os.environ["ANTHROPIC_BASE_URL"] = "https://b4u.qzz.io/"

# 初始化模型
def get_claude_llm():
    return ChatAnthropic(
        model="claude-4.5-sonnet",
        timeout=3000000  # 3000秒
    )

if __name__ == "__main__":
    # 使用模型
    from langchain_core.messages import HumanMessage

    messages = [HumanMessage(content="Hello, how are you?")]
    response = get_claude_llm().invoke(messages)
    print(response.content)