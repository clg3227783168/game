"""
SQL 检查节点实现
使用 LangChain 的 SQLDatabaseToolkit 和 sql_db_query_checker 检查 SQL 语句是否正确
"""

from typing import Dict, Any, Optional
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from llm_backend.getllm import get_claude_llm
from base_agent import BaseAgent


class SQLCheckerNode(BaseAgent):
    """
    SQL 检查节点 - 使用 LangChain 工具检查 SQL 语句的正确性

    功能：
    - 使用 SQLDatabaseToolkit 的 sql_db_query_checker 工具检查 SQL 语句的语法和逻辑正确性
    - 返回 True（正确）或 False（错误）
    """

    def __init__(self):
        """
        初始化 SQL 检查节点
        """
        db_uri = "mysql+pymysql://root@127.0.0.1:9030/database_name"

        # 连接到数据库
        try:
            self.db = SQLDatabase.from_uri(db_uri)
            print(f"✅ 成功连接到数据库")
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            raise
        # 创建 SQLDatabaseToolkit
        self.toolkit = SQLDatabaseToolkit(db=self.db, llm=get_claude_llm())

        # 获取 sql_db_query_checker 工具
        self.tools = self.toolkit.get_tools()
        self.query_checker = None

        for tool in self.tools:
            if tool.name == "sql_db_query_checker":
                self.query_checker = tool
                break

        if self.query_checker is None:
            print("⚠️ 警告: 未找到 sql_db_query_checker 工具")

    def check_sql(self, sql: str) -> bool:
        """
        检查 SQL 语句是否正确

        Args:
            sql: 待检查的 SQL 语句

        Returns:
            bool: True 表示 SQL 正确，False 表示 SQL 有误
        """
        try:
            # 使用 query_checker 工具检查 SQL
            result = self.query_checker.run(sql)

            # 分析检查结果
            # query_checker 会返回修正后的 SQL 或错误信息
            # 如果返回的结果中不包含 "error"、"invalid"、"incorrect" 等关键词，
            # 则认为 SQL 正确
            error_keywords = ['error', 'invalid', 'incorrect', 'syntax error', 'failed']
            has_error = any(keyword in result.lower() for keyword in error_keywords)

            if has_error:
                print(f"❌ SQL 检查失败: {result}")
                return False
            else:
                print(f"✅ SQL 检查通过")
                return True

        except Exception as e:
            print(f"❌ SQL 检查过程中出现异常: {e}")
            return False

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行 SQL 检查（LangGraph 兼容接口）

        Args:
            input_data: 输入数据，必须包含：
                - sql: 待检查的 SQL 语句

        Returns:
            结果字典，包含：
                - sql: 原始 SQL 语句
                - is_valid: 是否正确（True/False）
                - message: 检查信息
        """
        sql = input_data.get('sql', '')

        if not sql:
            return {
                'sql': '',
                'is_valid': False,
                'message': 'SQL 语句不能为空'
            }

        is_valid = self.check_sql(sql)

        return {
            'sql': sql,
            'is_valid': is_valid,
            'message': 'SQL 正确' if is_valid else 'SQL 有误'
        }


# LangGraph 兼容接口
def sql_checker_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    LangGraph 节点函数 - SQL 检查

    Args:
        state: LangGraph 状态字典，应包含：
            - sql: 待检查的 SQL 语句
            - db_uri 或 db_config: 数据库连接信息

    Returns:
        更新后的状态字典，添加了 sql_check_result 字段
    """
    # 提取数据库连接信息
    db_uri = state.get('db_uri', None)
    db_config = state.get('db_config', None)

    # 创建 SQL 检查节点
    node = SQLCheckerNode(db_uri=db_uri, db_config=db_config)

    # 执行检查
    check_result = node.run(state)

    # 更新状态
    new_state = state.copy()
    new_state['sql_check_result'] = check_result

    return new_state
