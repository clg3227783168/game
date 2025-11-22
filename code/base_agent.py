"""
Base Agent 基类
提供通用知识加载功能，供所有 Agent 节点继承使用
"""

import os
from pathlib import Path


class BaseAgent:
    """
    Agent 节点的基类

    功能：
    - 自动加载 common_knowledge.md 作为固有记忆
    """

    def __init__(self):
        """
        初始化基类，自动加载通用知识
        """
        with open(str(Path(__file__).parent / "data/common_knowledge.md"), 'r', encoding='utf-8') as f:
            content = f.read()
        self.common_knowledge = content
