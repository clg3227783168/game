"""
SQL 检查节点实现
使用 MySQL EXPLAIN 命令检查 SQL 语句是否正确
"""

import pymysql
from typing import Dict


class SQLCheckerNode:
    """
    SQL 检查节点

    使用 MySQL 的 EXPLAIN 命令来验证 SQL 语句的语法和语义正确性。
    EXPLAIN 不会实际执行 SQL，只检查语法、表名、列名等是否有效。
    """

    def __init__(self):
        """
        初始化 SQL 检查节点

        配置数据库连接参数（硬编码）
        """
        # 数据库连接配置
        self.db_config = {
            'host': '127.0.0.1',       # 数据库主机地址
            'user': 'root',            # 数据库用户名
            # 'password': '',          # 数据库密码（如果有密码请取消注释并填写）
            'db': 'database_name',     # 数据库名称
            'port': 9030               # 数据库端口（StarRocks 默认 9030）
        }

    def run(self, input_dict: Dict) -> Dict:
        """
        检查 SQL 语句的正确性

        Args:
            input_dict (dict): 输入字典，包含 'sql' 字段
                - sql (str): 需要检查的 SQL 语句

        Returns:
            dict: 检查结果
                - is_valid (bool): SQL 是否有效
                - message (str): 检查结果消息
        """
        sql = input_dict.get('sql', '').strip()

        # 检查 SQL 是否为空
        if not sql:
            return {
                'is_valid': False,
                'message': 'SQL 语句为空'
            }

        conn = None
        try:
            # 1. 连接数据库
            conn = pymysql.connect(**self.db_config)
            cursor = conn.cursor()

            # 2. 使用 EXPLAIN 检查 SQL
            # EXPLAIN 不会实际执行 SQL，只检查其有效性
            explain_sql = f"EXPLAIN {sql}"
            cursor.execute(explain_sql)

            # 3. 如果执行成功，说明 SQL 有效
            cursor.close()

            return {
                'is_valid': True,
                'message': 'SQL 检查通过'
            }

        except pymysql.err.ProgrammingError as e:
            # SQL 语法错误或表/列不存在
            error_code, error_msg = e.args
            return {
                'is_valid': False,
                'message': f'SQL 语法或语义错误 (错误码 {error_code}): {error_msg}'
            }

        except pymysql.err.OperationalError as e:
            # 数据库连接错误
            error_code, error_msg = e.args
            return {
                'is_valid': False,
                'message': f'数据库连接错误 (错误码 {error_code}): {error_msg}'
            }

        except Exception as e:
            # 其他未预期的错误
            return {
                'is_valid': False,
                'message': f'SQL 检查失败: {str(e)}'
            }

        finally:
            # 确保数据库连接被关闭
            if conn:
                conn.close()


# --- 测试代码 ---
if __name__ == '__main__':
    """
    测试 SQLCheckerNode 的功能
    """

    # 创建 SQL 检查节点
    checker = SQLCheckerNode()

    # 测试用例 1: 正确的 SQL
    test_sql_1 = "SELECT * FROM dim_mgamejp_tbplayerid2wxid_nf WHERE dtstatdate = 20240101"
    print("\n=== 测试 1: 正确的 SQL ===")
    print(f"SQL: {test_sql_1}")
    result = checker.run({'sql': test_sql_1})
    print(f"结果: {result}")

    # 测试用例 2: 错误的表名
    test_sql_2 = "SELECT * FROM non_existent_table WHERE id = 1"
    print("\n=== 测试 2: 表不存在 ===")
    print(f"SQL: {test_sql_2}")
    result = checker.run({'sql': test_sql_2})
    print(f"结果: {result}")

    # 测试用例 3: 语法错误
    test_sql_3 = "SELECT * FORM table_name"
    print("\n=== 测试 3: 语法错误 ===")
    print(f"SQL: {test_sql_3}")
    result = checker.run({'sql': test_sql_3})
    print(f"结果: {result}")

    # 测试用例 4: 空 SQL
    test_sql_4 = ""
    print("\n=== 测试 4: 空 SQL ===")
    print(f"SQL: '{test_sql_4}'")
    result = checker.run({'sql': test_sql_4})
    print(f"结果: {result}")
