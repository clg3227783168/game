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

# Prompt 模板
SCHEMA_LINKING_PROMPT_TEMPLATE = """你是一个精通 SQL 的数据库架构师。你的任务是将自然语言问题转换为结构化的数据库逻辑映射（Schema Linking）。

**【核心原则】**
1.  **知识即代码**：如果【业务知识】中包含了具体的 SQL 片段（如 `substr`, `case when`, `instr`），**必须直接保留并在输出中使用**，不要试图简化它。
2.  **逻辑完整性**：不仅仅识别列名，还要识别列名上的**操作**（如 `SUM`, `COUNT`, `Math Op`）。
3.  **消歧义**：明确区分“过滤条件”和“展示字段”。

---

**【系统固有知识】**
{common_knowledge}

**【业务逻辑 (必须严格遵守)】**
{knowledge}
*警告：业务逻辑中的常量定义（如 id="1001"）、计算公式（如 sum(amt)/100）、分类逻辑（如 case when...）必须被完整提取。*

---

**【数据库架构】**
**可用表：**
{table_list}

**详细表结构：**
{table_schemas}

---

**【执行指南与输出格式】**

请分析问题，提取以下五类标签（Tag），**每行一条**：

1.  **TIME** (Time Range):
    * 提取所有时间限制。
    * **格式**：`表名.日期列 BETWEEN '开始' AND '结束'` 或 `表名.日期列 = '日期'`。
    * *注意：将自然语言（如"近30天"）转换为具体日期范围。*

2.  **FILT** (Filter - 强约束):
    * WHERE 子句中的过滤条件。
    * **格式**：`表名.列名 操作符 '值'` 或 `SQL表达式 操作符 '值'`。
    * *示例*：`t1.sgamecode IN ('initiatived', 'jordass')`
    * *示例*：`instr(substr(cbitmap,1,7),'1') > 0` (直接复制业务知识中的逻辑)

3.  **SELC** (Select - 展示与计算):
    * SELECT 子句中需要的列或**计算表达式**。
    * **格式**：`表名.列名` 或 `聚合函数(表名.列名)` 或 `SQL表达式 AS 别名`。
    * *示例*：`SUM(t1.ionlinetime)`
    * *示例*：`CASE WHEN tier >= 24 THEN '神话' ELSE '其他' END`

4.  **LINK** (Join):
    * 表间关联条件。
    * **格式**：`表名1.列名1 = 表名2.列名2`。

5.  **GRUP** (Group By):
    * 分组依据的列（通常是 SELC 中非聚合的原始列）。

---

**【参考范例 (Few-Shot Examples)】**
Example 1: 复杂筛选与 SQL 片段 (覆盖 Knowledge 中的 SQL)
Input Question: 统计2025年5月勇者盟约流失用户中，流向峡谷PC的玩家gplayerid Input Knowledge: 5月流失：限定INSTR(SUBSTR(REVERSE(RPAD(iactivity,128,'0')),1,31),'1') = 0... 取20250608的数据 Response:
Output:
TIME: dws_user_retention.dtstatdate = '20250608'
FILT: dws_user_retention.sgamecode = 'initiatived'
FILT: dws_user_retention.itimes >= 1
FILT: dws_user_retention.saccounttype = '-100'
FILT: dws_user_retention.splattype = 'PC'
FILT: INSTR(SUBSTR(REVERSE(RPAD(dws_user_retention.iactivity,128,'0')),1,31),'1') = 0
FILT: INSTR(SUBSTR(REVERSE(RPAD(dws_user_retention.iactivity,128,'0')),32,31),'1') > 0
SELC: dws_user_retention.gplayerid

Example 2: 映射逻辑与聚合 (覆盖 CASE WHEN 和 GROUP BY)
Input Question: 统计2023/7/12-2025/5/31按段位统计所有竞技模式对局中，排除掉投降的对局情况。 Input Knowledge: 段位筛选逻辑：case when competitiveTier in (3, 4, 5) then "黑铁"... 竞技模式：queueId="competitive" Response:
Output:
TIME: dws_match_detail.dtstatdate BETWEEN '20230712' AND '20250531'
FILT: dws_match_detail.queueId = 'competitive'
FILT: lower(dws_match_detail.completionState) NOT IN ('surrendered')
SELC: dws_match_detail.dtstatdate
SELC: CASE WHEN competitiveTier IN (3,4,5) THEN '黑铁' WHEN competitiveTier IN (6,7,8) THEN '黄铜' ELSE '无段位' END AS tier_name
SELC: COUNT(1)
SELC: SUM(CASE WHEN roundsdiff >= 6 THEN 1 ELSE 0 END)
GRUP: dws_match_detail.dtstatdate
GRUP: dws_match_detail.competitiveTier

Example 3: 派生指标与日期计算 (覆盖 LTV/留存)
Input Question: 统计2024.01.01-2024.01.31每日新增用户的LTV1~LTV3。 Input Knowledge: LTV：累计产生的总流水 ÷ 总人数。 Response:
Output:
TIME: dim_user_reg.reg_date BETWEEN '20240101' AND '20240131'
SELC: dim_user_reg.reg_date
SELC: COUNT(DISTINCT dim_user_reg.userid) AS new_users
SELC: SUM(CASE WHEN datediff(pay_date, reg_date) <= 0 THEN pay_amount ELSE 0 END) / COUNT(DISTINCT userid) AS LTV1
SELC: SUM(CASE WHEN datediff(pay_date, reg_date) <= 2 THEN pay_amount ELSE 0 END) / COUNT(DISTINCT userid) AS LTV3
LINK: dim_user_reg.userid = dws_pay_detail.userid
GRUP: dim_user_reg.reg_date

---

**【待处理任务】**

**问题描述：**
{question}

**输出：**
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
            result.append(f"\n表: {table_name} {table_info.get('table_description', 'N/A')}")
            for col in table_info['columns']:
                result.append(f"  - {col['col']} ({col['type']}): {col['description']}")

        return '\n'.join(result)

    def run(self, input_data: Dict[str, Any]):
        # 获取表结构信息（用于 LLM prompt）
        table_schemas_full = self._get_tables_info(input_data['table_list'])
        # 构建输入
        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': input_data['question'],
            'table_list': ', '.join(input_data['table_list']),
            'knowledge': input_data.get('knowledge', ''),
            'table_schemas': table_schemas_full
        }
        # print(self.prompt.format(**prompt_input))
        # print("=================================")
        # 调用 LLM 并处理
        raw_response = self.chain.invoke(prompt_input)
        response_text = raw_response.content if hasattr(raw_response, 'content') else str(raw_response)
        schema_links = self._parse_schema_links(response_text)

        # 从 schema_links 中提取表列信息
        table_cols = self._extract_table_columns_from_links(schema_links)
        # 只返回 schema_links 中提到的描述信息, 如果未能提取则退回使用完整表结构
        filtered_table_schemas = self._get_filtered_tables_info(table_cols) or table_schemas_full

        return {
            "schema_links": schema_links,
            "table_schemas": filtered_table_schemas
        }

    def _extract_table_columns_from_links(self, schema_links: str) -> Dict[str, List[str]]:
        """
        从 Schema Linking 的文本结果中解析出涉及的表和列
        返回格式: {table_name: [col1, col2, ...]}
        """
        if not schema_links:
            return {}

        # 构建表名和列名的小写索引，提升匹配鲁棒性
        table_lookup = {table.lower(): table for table in self.table_columns}
        column_lookup = {
            table: {col.lower(): col for col in cols}
            for table, cols in self.table_columns.items()
        }

        pattern = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)')
        extracted: Dict[str, set] = {}

        for table_raw, col_raw in pattern.findall(schema_links):
            table_key = table_lookup.get(table_raw.lower())
            if not table_key:
                continue

            # 只保留 schema 中存在的列，避免错误列名干扰
            normalized_col = column_lookup[table_key].get(col_raw.lower())
            if not normalized_col:
                continue

            extracted.setdefault(table_key, set()).add(normalized_col)

        # 按 schema 中的顺序返回列列表，保持输出稳定
        ordered_result: Dict[str, List[str]] = {}
        for table, cols in extracted.items():
            ordered_cols = [col for col in self.table_columns[table] if col in cols]
            ordered_result[table] = ordered_cols

        return ordered_result

    def _get_filtered_tables_info(self, table_cols: Dict[str, List[str]]) -> str:
        """
        根据提取到的表-列映射，返回对应的表结构描述字符串。
        如果某个表未提取到具体列，则返回空字符串，由调用方决定回退策略。
        """
        if not table_cols:
            return ""

        result_lines = []
        for table_name, cols in table_cols.items():
            table_info = self.tables.get(table_name)
            if not table_info:
                continue

            filtered_cols = set(cols)
            if not filtered_cols:
                # 如果没有具体列，则跳过该表，由上游回退到完整 schema
                continue

            result_lines.append(f"\n表名: {table_name}")
            result_lines.append(f"描述: {table_info.get('table_description', 'N/A')}")
            result_lines.append("列名: (数据类型): 描述")

            # 仅输出提取到的列，顺序沿用原 schema
            for col in table_info['columns']:
                if col['col'] in filtered_cols:
                    result_lines.append(f"  - {col['col']} ({col['type']}): {col['description']}")

        return '\n'.join(result_lines)

    def _parse_schema_links(self, llm_output):
        """
        从 LLM 的输出中提取结构化标签，忽略前面的分析废话。
        兼容格式：
        TIME: ...
        **TIME**: ...
        - TIME: ...
        """
        # 定义合法的标签头
        valid_tags = ["TIME", "FILT", "SELC", "LINK", "GRUP", "ORDR", "COLUMN", "FILTER", "JOIN"]
        
        pattern = re.compile(r"^[\*\-\s]*\**\s*(" + "|".join(valid_tags) + r")\s*\**\s*[:：]\s*(.*)", re.IGNORECASE | re.MULTILINE)
        
        parsed_lines = []
        matches = pattern.findall(llm_output)
        
        for tag, content in matches:
            # 标准化输出格式：全大写标签: 内容
            clean_tag = tag.upper()
            clean_content = content.strip()
            # 某些生成内容可能在末尾带有 "\"}" 这样的收尾符，去掉它保持输出整洁
            clean_content = re.sub(r'"?}\s*$', "", clean_content)
            parsed_lines.append(f"{clean_tag}: {clean_content}")
            
        return "\n".join(parsed_lines)

if __name__ == "__main__":
    # 测试
    test_data ={
        "sql_id": "sql_3",
        "question": "统计2025年1月勇者盟约端游活跃玩家交叉峡谷端游及手游活跃玩家\n输出：玩家gplayerid",
        "复杂度": "简单",
        "table_list": [
            "dws_argothek_oss_login_di",
            "dim_argothek_gplayerid2qqwxid_df",
            "dws_mgamejp_login_user_activity_di"
        ],
        "knowledge": "峡谷筛选逻辑:\nsgamecode = \"initiatived\" -- 筛选峡谷游戏\nand saccounttype = \"-100\" -- 账号体系，取-100表示汇总\nand suseridtype in (\"qq\", \"wxid\") -- 账号类型，取qq或wxid\nand splattype in (\"-100\", \"PC\") -- 峡谷手游玩家及PC端玩家\nand splat = \"-100\" -- 写死为-100"
    }

    node = SchemaLinkingNode()
    output = node.run(test_data)
    # print(output)
    print(output['schema_links'])
    print("*******************************************")
    print(output['table_schemas'])
