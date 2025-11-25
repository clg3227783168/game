"""
Schema Linking 节点实现
从自然语言问题中识别需要使用的表.列、JOIN关系和具体值
"""

import json
from typing import List, Dict, Any
from pathlib import Path
import re
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from llm_backend.getllm import get_claude_llm
from base_agent import BaseAgent
from schema_linking_check import SchemaValidator

# Prompt 模板
SCHEMA_LINKING_PROMPT_TEMPLATE = """你是一个专业的 SQL Schema Linking 专家。

**【系统固有知识 - 请牢记】**
{common_knowledge}

**业务知识：**
{knowledge}

---

给定以下信息：

**问题描述：**
{question}

**可用表：**
{table_list}

**表结构详情：**
{table_schemas}

---

**任务：**
请识别出生成 查询类SQL 语句时需要使用的所有：
1. 表.列（格式：table_name.column_name）
2. JOIN 关系（格式：table1.column1=table2.column2）
3. 具体的值（如日期、字符串常量等）

**输出要求：**
- 每行一个元素
- 不要包含序号或其他格式
- 只输出纯粹的 schema links
- 禁止输出任何解释性文字

**示例输出：**
```
dws_mgamejp_login_user_activity_di.suserid
dws_mgamejp_login_user_activity_di.sgamecode
dws_mgamejp_login_user_activity_di.vplayerid=dim_vplayerid_vies_df.vplayerid
2025-07-24
initiatived
jordass
```

现在开始识别：
"""


class SchemaLinkingNode(BaseAgent):
    """Schema Linking 节点 - 从问题中识别需要的表、列和值"""

    def __init__(self):
        """初始化 Schema Linking 节点"""
        super().__init__()

        # Schema 数据结构
        self.tables: Dict[str, Dict] = {}
        self.table_columns: Dict[str, List[str]] = {}
        # 加载 schema
        with open(str(Path(__file__).parent / "data/schema.json"), 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        # 建立索引
        for table in schema_data:
            table_name = table['table_name']
            self.tables[table_name] = table
            self.table_columns[table_name] = [col['col'] for col in table['columns']]

        self.prompt = PromptTemplate(
            template=SCHEMA_LINKING_PROMPT_TEMPLATE,
            input_variables=["common_knowledge", "question", "table_list", "knowledge", "table_schemas"]
        )
        self.chain = self.prompt | get_claude_llm() | StrOutputParser()

    def _get_tables_info(self, table_names: List[str]) -> str:
        """获取多个表的结构信息，格式化为可读字符串"""
        result = []
        for table_name in table_names:
            table_info = self.tables[table_name]
            result.append(f"\n表名: {table_name}")
            result.append(f"描述: {table_info.get('table_description', 'N/A')}")
            result.append("列:")
            for col in table_info['columns']:
                result.append(f"  - {col['col']} ({col['type']}): {col['description']}")

        return '\n'.join(result)

    def run(self, input_data: Dict[str, Any]):
        """执行 Schema Linking，返回验证后的 schema links 列表"""
        # 获取表结构信息
        table_schemas = self._get_tables_info(input_data['table_list'])
        # 构建输入
        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': input_data['question'],
            'table_list': ', '.join(input_data['table_list']),
            'knowledge': input_data.get('knowledge', ''),
            'table_schemas': table_schemas
        }

        # 调用 LLM 并处理
        response = self.chain.invoke(prompt_input)
        validated = SchemaValidator(self.table_columns)
        return {
            "schema_links": validated.validate_and_filter(self._parse_response(response)),
            "table_schemas": table_schemas
        }

    def _parse_response(self, response: str) -> List[str]:
        """解析 LLM 响应，提取 schema links"""
        schema_links = []
        for line in response.strip().split('\n'):
            line = line.strip()
            # 跳过空行和标记
            if not line or line.startswith('```') or line.startswith('#'):
                continue
            # 去除序号
            if line and line[0].isdigit() and '.' in line[:5]:
                line = line.split('.', 1)[1].strip()
            if line.startswith('- '):
                line = line[2:].strip()
            if line:
                schema_links.append(line)
        return schema_links

if __name__ == "__main__":
    # 测试
    test_data ={
        "sql_id": "sql_1",
        "question": "统计2025.07.24的手游全量用户且标签为其他，在竞品业务下2025.05.30-2025.07.24的在线时长。\n输出：suserid、sgamecode、ionlinetime\n\n",
        "复杂度": "中等",
        "table_list": [
            "dws_mgamejp_login_user_activity_di",
            "dim_vplayerid_vies_df"
        ],
        "knowledge": "竞品业务：\nsgamecode in (\"initiatived\",\"jordass\",\"esports\",\"allianceforce\",\"strategy\",\"playzone\",\"su\")\nsaccounttype = \"-100\" -- 账号体系，取-100表示汇总\nand suseridtype in (\"qq\",\"wxid\") -- 用户类型\nand splattype = \"-100\" -- 平台类型\nand splat = \"-100\" -- 平台，写死为-100\n"
    }

    node = SchemaLinkingNode()
    output = node.run(test_data)
    print(output['schema_links'])
