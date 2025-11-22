"""
SQL验证模块
提供SQL语法检查和数据库执行验证功能
"""

import re
import json
import pymysql
from typing import Dict, List, Tuple, Optional
from pathlib import Path


class SQLValidator:
    """SQL验证器类"""

    def __init__(self, schema_path: str = "code/data/schema.json"):
        """
        初始化SQL验证器

        Args:
            schema_path: 数据库schema文件路径
        """
        self.schema = self._load_schema(schema_path)
        self.table_columns = self._build_table_columns_dict()

    def _load_schema(self, path: str) -> List[Dict]:
        """加载schema文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _build_table_columns_dict(self) -> Dict[str, List[str]]:
        """构建表名到列名的映射字典"""
        table_columns = {}
        for table in self.schema:
            table_name = table['table_name']
            columns = [col['column_name'] for col in table.get('columns', [])]
            table_columns[table_name] = columns
        return table_columns

    def extract_table_names(self, sql: str) -> List[str]:
        """
        从SQL中提取表名

        Args:
            sql: SQL语句

        Returns:
            表名列表
        """
        # 移除注释和换行符
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # 提取FROM和JOIN后的表名
        # 匹配模式：FROM table_name 或 JOIN table_name
        pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)'
        matches = re.findall(pattern, sql, re.IGNORECASE)

        # 去重并返回
        return list(set(matches))

    def extract_column_names(self, sql: str) -> List[str]:
        """
        从SQL中提取列名（简单版本）

        Args:
            sql: SQL语句

        Returns:
            可能的列名列表
        """
        # 这是一个简化版本，主要提取SELECT后和WHERE子句中的列名
        # 移除注释
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

        # 提取可能的列名（table.column 或 column）
        pattern = r'\b([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+|(?<=[^\w])[a-zA-Z_][a-zA-Z0-9_]*(?=\s*[,=<>)]|\s+(?:AS|FROM|WHERE|GROUP|ORDER|HAVING)))'
        matches = re.findall(pattern, sql, re.IGNORECASE)

        # 过滤掉SQL关键字
        sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'BY', 'HAVING',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS',
            'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        }

        filtered = [m for m in matches if m.upper() not in sql_keywords]
        return list(set(filtered))

    def validate_syntax(self, sql: str, expected_tables: Optional[List[str]] = None) -> Dict:
        """
        验证SQL语法（检查表名和列名是否存在）

        Args:
            sql: SQL语句
            expected_tables: 预期使用的表列表（可选）

        Returns:
            验证结果字典，包含：
            - valid: 是否通过验证
            - errors: 错误列表
            - warnings: 警告列表
            - extracted_tables: 提取的表名
        """
        errors = []
        warnings = []

        # 提取表名
        extracted_tables = self.extract_table_names(sql)

        # 验证表名是否存在
        invalid_tables = [t for t in extracted_tables if t not in self.table_columns]
        if invalid_tables:
            errors.append(f"未找到的表: {', '.join(invalid_tables)}")

        # 如果提供了预期表列表，检查是否匹配
        if expected_tables:
            missing_tables = set(expected_tables) - set(extracted_tables)
            extra_tables = set(extracted_tables) - set(expected_tables)

            if missing_tables:
                warnings.append(f"预期的表未在SQL中使用: {', '.join(missing_tables)}")
            if extra_tables:
                warnings.append(f"SQL中使用了预期外的表: {', '.join(extra_tables)}")

        # 提取列名并验证（仅对有效的表）
        valid_tables = [t for t in extracted_tables if t in self.table_columns]
        if valid_tables:
            extracted_columns = self.extract_column_names(sql)

            # 验证table.column格式的列名
            for col_ref in extracted_columns:
                if '.' in col_ref:
                    table, column = col_ref.split('.', 1)
                    if table in self.table_columns:
                        if column not in self.table_columns[table]:
                            errors.append(f"表 {table} 中未找到列: {column}")

        # 基本SQL语法检查
        if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
            errors.append("SQL语句缺少SELECT关键字")

        if not re.search(r'\bFROM\b', sql, re.IGNORECASE):
            errors.append("SQL语句缺少FROM关键字")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'extracted_tables': extracted_tables
        }

    def execute_sql(self, sql: str, db_config: Dict, limit: int = 10) -> Dict:
        """
        执行SQL并返回结果

        Args:
            sql: SQL语句
            db_config: 数据库配置
            limit: 限制返回行数（默认10行，避免数据量过大）

        Returns:
            执行结果字典，包含：
            - success: 是否执行成功
            - result: 查询结果（成功时）
            - error: 错误信息（失败时）
            - row_count: 返回行数
        """
        conn = None
        try:
            # 在SQL末尾添加LIMIT（如果没有）
            sql_with_limit = sql.strip()
            if not re.search(r'\bLIMIT\s+\d+', sql_with_limit, re.IGNORECASE):
                # 移除末尾的分号
                sql_with_limit = sql_with_limit.rstrip(';')
                sql_with_limit = f"{sql_with_limit} LIMIT {limit}"

            # 连接数据库
            conn = pymysql.connect(**db_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 执行SQL
            cursor.execute(sql_with_limit)
            result = cursor.fetchall()

            return {
                'success': True,
                'result': result,
                'row_count': len(result),
                'error': None
            }

        except pymysql.Error as e:
            return {
                'success': False,
                'result': None,
                'row_count': 0,
                'error': str(e)
            }

        except Exception as e:
            return {
                'success': False,
                'result': None,
                'row_count': 0,
                'error': f"执行异常: {str(e)}"
            }

        finally:
            if conn:
                conn.close()

    def validate_sql_full(self, sql: str, expected_tables: Optional[List[str]] = None,
                         db_config: Optional[Dict] = None) -> Dict:
        """
        完整验证SQL（语法检查 + 数据库执行）

        Args:
            sql: SQL语句
            expected_tables: 预期使用的表列表
            db_config: 数据库配置（如果提供则执行数据库验证）

        Returns:
            完整验证结果
        """
        result = {
            'sql': sql,
            'syntax_check': None,
            'execution_check': None,
            'overall_valid': False
        }

        # 1. 语法检查
        syntax_result = self.validate_syntax(sql, expected_tables)
        result['syntax_check'] = syntax_result

        # 2. 数据库执行验证（如果提供了db_config且语法检查通过）
        if db_config and syntax_result['valid']:
            execution_result = self.execute_sql(sql, db_config)
            result['execution_check'] = execution_result
            result['overall_valid'] = execution_result['success']
        else:
            result['overall_valid'] = syntax_result['valid']

        return result


def batch_validate_sqls(sql_list: List[Dict], validator: SQLValidator,
                        db_config: Optional[Dict] = None) -> Tuple[List[Dict], List[Dict]]:
    """
    批量验证SQL列表

    Args:
        sql_list: SQL数据列表，每项包含 sql, table_list 等字段
        validator: SQLValidator实例
        db_config: 数据库配置

    Returns:
        (通过验证的列表, 未通过验证的列表)
    """
    valid_sqls = []
    invalid_sqls = []

    total = len(sql_list)
    for idx, item in enumerate(sql_list, 1):
        print(f"验证进度: {idx}/{total} - {item.get('sql_id', 'unknown')}")

        sql = item.get('sql', '')
        table_list = item.get('table_list', [])

        # 执行完整验证
        validation_result = validator.validate_sql_full(
            sql,
            expected_tables=table_list,
            db_config=db_config
        )

        # 将验证结果添加到item中
        item['validation_result'] = validation_result

        if validation_result['overall_valid']:
            valid_sqls.append(item)
        else:
            invalid_sqls.append(item)

    print(f"\n验证完成:")
    print(f"  - 通过: {len(valid_sqls)}/{total}")
    print(f"  - 失败: {len(invalid_sqls)}/{total}")

    return valid_sqls, invalid_sqls


if __name__ == "__main__":
    # 测试代码
    validator = SQLValidator()

    # 测试SQL
    test_sql = """
    SELECT vplayerid, COUNT(*) as login_count
    FROM dws_jordass_login_di
    WHERE dtstatdate >= '2024-01-01'
    GROUP BY vplayerid
    """

    # 语法验证
    result = validator.validate_syntax(test_sql, expected_tables=['dws_jordass_login_di'])
    print("语法验证结果:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 如果要测试数据库执行，取消下面的注释并配置数据库
    # db_config = {
    #     'host': '127.0.0.1',
    #     'user': 'root',
    #     'password': '',
    #     'db': 'database_name',
    #     'port': 3306
    # }
    # full_result = validator.validate_sql_full(test_sql, db_config=db_config)
    # print("\n完整验证结果:")
    # print(json.dumps(full_result, ensure_ascii=False, indent=2))
