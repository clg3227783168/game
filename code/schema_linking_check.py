"""
Schema Linking 检查模块
负责验证 schema links 的正确性
"""

from typing import List, Dict, Optional


class SchemaValidator:
    """Schema 验证器 - 负责验证 schema links 的正确性"""

    def __init__(self, table_columns: Dict[str, List[str]]):
        """
        初始化 Schema 验证器

        Args:
            table_columns: 表名到列名列表的映射 {table_name: [columns]}
        """
        self.table_columns = table_columns

    def validate_column(self, table_name: str, column_name: str) -> bool:
        """
        验证列是否存在于指定表中

        Args:
            table_name: 表名
            column_name: 列名

        Returns:
            True 如果列存在，否则 False
        """
        if table_name not in self.table_columns:
            return False
        return column_name in self.table_columns[table_name]

    def validate_schema_link(self, schema_link: str) -> bool:
        """
        验证一个 schema link 是否有效

        Args:
            schema_link: 形如 "table.column" 或 "table1.col1=table2.col2"

        Returns:
            True 如果有效，否则 False
        """
        # 如果不包含 '.'，可能是值，直接返回 True
        if '.' not in schema_link:
            return True

        # 处理 JOIN 关系（table1.col1=table2.col2）
        if '=' in schema_link:
            parts = schema_link.split('=')
            if len(parts) != 2:
                return False
            return all(self._validate_table_column(part.strip()) for part in parts)

        # 处理普通的 table.column
        return self._validate_table_column(schema_link)

    def _validate_table_column(self, table_column: str) -> bool:
        """
        验证 table.column 格式的字符串

        Args:
            table_column: 形如 "table.column" 的字符串

        Returns:
            True 如果格式正确且表列存在，否则 False
        """
        parts = table_column.split('.')
        if len(parts) != 2:
            return False
        table_name, column_name = parts
        return self.validate_column(table_name.strip(), column_name.strip())

    def validate_and_filter(self, schema_links: List[str]) -> List[str]:
        """
        验证并过滤 schema links 列表

        Args:
            schema_links: 原始的 schema links 列表

        Returns:
            验证后的 schema links 列表（过滤掉无效的）
        """
        validated = []

        for link in schema_links:
            # 验证 schema link
            if self.validate_schema_link(link):
                validated.append(link)
            else:
                print(f"警告: Schema link 验证失败，已过滤: {link}")

        return validated


def create_validator_from_schema(schema_data: List[Dict]) -> SchemaValidator:
    """
    从 schema 数据创建验证器

    Args:
        schema_data: schema.json 的数据（表列表）

    Returns:
        SchemaValidator 实例
    """
    table_columns = {}
    for table in schema_data:
        table_name = table['table_name']
        table_columns[table_name] = [col['col'] for col in table['columns']]

    return SchemaValidator(table_columns)
