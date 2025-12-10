"""
SQL生成节点 - 基于Schema Linking结果和参考案例生成SQL
利用true.json中的正确SQL案例进行Few-Shot Learning
改进后的版本：使用文本输出 + 正则解析，提高容错性
"""

import json
import os
import re
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from langchain_core.prompts import PromptTemplate
from llm_backend.getllm import get_claude_llm
from base_agent import BaseAgent
from pathlib import Path

SQL_GENERATION_PROMPT = """你是一个精通 **StarRocks (兼容 MySQL 协议)** 的数据仓库专家。
你的任务是根据已知线索，编写准确、高效的 StarRocks SQL 查询语句。

**【系统固有知识】**
{common_knowledge}

---

**【数据库方言与规范 (StarRocks)】**

1.  **语法基础**：使用标准 SQL-92/99 语法，兼容 MySQL。
    - 引用表名/列名时使用反引号 (例如: `column_name`)。
    - 字符串使用单引号 (例如: 'value')。
    - 别名使用 AS (例如: `SELECT col AS c`).
2.  **日期格式 (高危)**：
    - 此数据库中的日期分区字段通常为字符串格式 **'YYYYMMDD'** (无连字符)。
    - 示例：`WHERE ds = '20251210'` (正确)，`WHERE ds = '2025-12-10'` (错误)。
    - 如果需要进行日期计算，请使用 `date_add`, `date_sub`, `datediff`，但在与 `ds` 列比较前，必须格式化为 'YYYYMMDD' 字符串。
3.  **分区裁剪**：
    - 查询大表时，**必须**在 WHERE 子句中指定分区列（通常是 `ds` 或 `dtstatdate`）的过滤条件，否则禁止生成 SQL。

---

**【输入信息】**

1.  **用户问题：**
    {question}

2.  **Schema Linking 结果 (结构化线索)：**
    {schema_links}
    *(说明: TIME=必须用于分区过滤的时间条件, FILT=强制过滤条件, SELC=展示列, LINK=关联条件)*

3.  **业务知识 (SQL片段)：**
    {knowledge}

4.  **表结构信息：**
    {table_schemas}
{error_feedback}

---

**【生成步骤】**

1.  **Step 1: 思考 (Thought)**
    - 检查 `TIME` 标签：识别查询涉及的具体日期范围。
    - 检查表结构中的分区字段（如 `ds`）：确认是否需要将日期转换为 'YYYYMMDD' 格式。
    - 规划 Join 路径和 Group By 逻辑。

2.  **Step 2: 编写 SQL**
    - 优先处理 `WHERE` 子句，确保分区裁剪。
    - 如果涉及 `knowledge` 中的复杂逻辑（如 `CASE WHEN`），直接复用。
    - 确保 SQL 语法符合 StarRocks 要求。

**【输出格式】**

Thought:
[你的逻辑分析，特别是关于日期格式转换的确认]

```sql
[完整的 StarRocks SQL 语句]
"""

class SQLGenerationNode(BaseAgent):
    """SQL生成节点 - 基于Schema Linking和参考案例生成SQL"""

    def __init__(self):
        """
        初始化SQL生成节点
        """
        super().__init__()

        # 加载 false2true.json 映射表
        with open(str(Path(__file__).parent / "data" / "false2true.json"), 'r', encoding='utf-8') as f:
            self.false2true_map = json.load(f)

        # 加载完整数据集（用于根据sql_id获取案例详情）
        with open(str(Path(__file__).parent / "data" / "final_dataset.json"), 'r', encoding='utf-8') as f:
            dataset = json.load(f)
            # 构建 sql_id -> 案例 的索引
            self.sql_id_to_case = {item['sql_id']: item for item in dataset}

        # 使用原始 LLM（不再使用 structured_output）
        self.llm = get_claude_llm()

        # 创建prompt模板
        self.prompt = PromptTemplate(
            template=SQL_GENERATION_PROMPT,
            input_variables=[
                'common_knowledge',
                'question',
                'knowledge',
                'schema_links',
                'table_schemas',
                'reference_case',
                'error_feedback'
            ]
        )

        # 创建链：prompt -> llm
        self.chain = self.prompt | self.llm

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成SQL语句（文本输出 + 正则解析）

        Args:
            input_data: 输入数据，应包含：
                - question: 问题描述
                - sql_id: SQL标识符，用于检索相似案例
                - table_list: 表列表
                - schema_links: Schema Linking结果
                - knowledge: 业务知识
                - table_schemas: 表结构信息
                - error_history: 错误历史列表（可选）

        Returns:
            结构化的SQL生成结果字典，包含以下字段：
                - sql: 生成的SQL语句
            失败时额外包含：
                - explanation: 失败说明
                - notes: 补充说明
                - raw_response: LLM的原始响应（用于调试）
        """
        question = input_data.get('question')
        sql_id = input_data.get('sql_id')
        table_list = input_data.get('table_list')
        schema_links = input_data.get('schema_links')
        knowledge = input_data.get('knowledge', '')
        table_schemas = input_data.get('table_schemas')
        error_history = input_data.get('error_history', [])

        # formatted_case = self._format_reference_case(self.sql_id_to_case.get(self.false2true_map.get(sql_id, ''), ''))

        # 格式化错误反馈
        error_feedback = self._format_error_feedback(error_history)

        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': question,
            'knowledge': knowledge if knowledge else '无',
            'schema_links': schema_links,
            'table_schemas': table_schemas,
            # 'reference_case': formatted_case,
            'error_feedback': error_feedback
        }

        # 打印完整的prompt内容
        # print("\n" + "="*80)
        # print("完整 Prompt:")
        # print("="*80)
        # formatted_prompt = self.prompt.format(**prompt_input)
        # print(formatted_prompt)
        # print("="*80 + "\n")

        response = self.chain.invoke(prompt_input)

        # 获取响应文本
        response_text = response.content if hasattr(response, 'content') else str(response)

        # 解析提取SQL
        sql = self._extract_sql(response_text)

        return {
            "sql": sql
        }

    def _extract_sql(self, response_text: str) -> str:
        """
        从LLM响应中提取SQL语句

        支持多种格式：
        1. Markdown代码块：```sql ... ```
        2. 直接的SQL语句（以SELECT、WITH、INSERT等开头）
        3. 包含其他文本的混合格式

        Args:
            response_text: LLM的原始响应文本

        Returns:
            提取出的SQL语句，清理后的纯SQL字符串
        """
        if not response_text or not response_text.strip():
            return ""

        # 方法1：尝试提取markdown代码块中的SQL
        # 匹配 ```sql ... ``` 或 ``` ... ```
        code_block_pattern = r'```(?:sql)?\s*\n?(.*?)\n?```'
        code_blocks = re.findall(code_block_pattern, response_text, re.DOTALL | re.IGNORECASE)

        if code_blocks:
            # 找到代码块，使用第一个
            sql = code_blocks[0].strip()
            if sql:
                return sql

        # 方法2：尝试提取以SQL关键字开头的语句
        # 匹配常见的SQL开头关键字
        sql_keywords = [
            r'(?:WITH\s+\w+\s+AS)',  # CTE
            r'(?:SELECT)',
            r'(?:INSERT)',
            r'(?:UPDATE)',
            r'(?:DELETE)',
            r'(?:CREATE)',
            r'(?:DROP)',
            r'(?:ALTER)'
        ]

        for keyword in sql_keywords:
            # 尝试匹配从关键字到结束的所有内容
            pattern = fr'({keyword}.*?)(?:$|\n\n|(?=\n[^-\s]))'
            match = re.search(pattern, response_text, re.DOTALL | re.IGNORECASE)
            if match:
                sql = match.group(1).strip()
                # 清理可能的尾部说明文字（如果有多个换行符）
                sql = re.split(r'\n\n+', sql)[0].strip()
                if sql:
                    return sql

        # 方法3：如果以上都失败，尝试清理文本后直接返回
        # 移除明显的非SQL文本（如"根据问题需求"、"我将生成"等）
        cleaned = re.sub(r'^.*?(?=(?:SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER))', '',
                        response_text, flags=re.DOTALL | re.IGNORECASE)

        if cleaned and cleaned != response_text:
            return cleaned.strip()

        # 如果所有方法都失败，返回原始文本（可能整个就是SQL）
        return response_text.strip()

    def _format_error_feedback(self, error_history: List[Dict[str, Any]]) -> str:
        """
        格式化错误历史为Prompt字符串

        Args:
            error_history: 错误历史列表，每项包含:
                - sql: 失败的SQL
                - error_message: 错误信息
                - retry_count: 重试次数

        Returns:
            格式化的错误反馈字符串
        """
        if not error_history:
            return ""

        feedback_lines = ["\n**历史错误反馈（必须针对这些错误进行修正）：**\n"]

        for i, error in enumerate(error_history, 1):
            sql = error.get('sql', '')
            error_msg = error.get('error_message', '')
            retry = error.get('retry_count', 0)

            # 截断过长的SQL（保留前200字符）
            sql_preview = sql[:200] + "..." if len(sql) > 200 else sql

            feedback_lines.append(f"**第 {i} 次尝试：**")
            feedback_lines.append(f"生成的SQL:")
            feedback_lines.append(f"```sql")
            feedback_lines.append(sql_preview)
            feedback_lines.append(f"```")
            feedback_lines.append(f"错误信息: {error_msg}")
            feedback_lines.append("")

        feedback_lines.append("**请根据上述错误，分析问题根因并生成正确的SQL！**")

        return "\n".join(feedback_lines)

    def _format_reference_case(self, case: Dict[str, Any]) -> str:
        """
        格式化参考案例为Prompt字符串

        Args:
            case: 相似案例

        Returns:
            格式化的参考案例字符串
        """
        return f"""
问题: {case.get('question', 'N/A')}
SQL: {case.get('sql', 'N/A')}
"""

if __name__ == "__main__":
    # 测试SQL生成节点
    sql_gen_node = SQLGenerationNode()

    # 示例输入数据
    input_data = {
        "sql_id": "sql_3",
        "question": "统计2025年1月勇者盟约端游活跃玩家交叉峡谷端游及手游活跃玩家\n输出：玩家gplayerid",
        "复杂度": "简单",
        "table_list": [
            "dws_argothek_oss_login_di",
            "dim_argothek_gplayerid2qqwxid_df",
            "dws_mgamejp_login_user_activity_di"
        ],
        "schema_links": """TIME: dws_argothek_oss_login_di.statis_date BETWEEN '20250101' AND '20250131'
TIME: dws_mgamejp_login_user_activity_di.dtstatdate BETWEEN '20250101' AND '20250131'
FILT: dws_mgamejp_login_user_activity_di.sgamecode = 'initiatived'
FILT: dws_mgamejp_login_user_activity_di.saccounttype = '-100'
FILT: dws_mgamejp_login_user_activity_di.suseridtype IN ('qq', 'wxid')
FILT: dws_mgamejp_login_user_activity_di.splattype IN ('-100', 'PC')
FILT: dws_mgamejp_login_user_activity_di.splat = '-100'
FILT: dws_mgamejp_login_user_activity_di.itimes >= 1
FILT: dws_argothek_oss_login_di.ilogincount >= 1
SELC: dim_argothek_gplayerid2qqwxid_df.iuserid
LINK: dws_argothek_oss_login_di.iuserid = dim_argothek_gplayerid2qqwxid_df.iuserid
LINK: dim_argothek_gplayerid2qqwxid_df.suserid = dws_mgamejp_login_user_activity_di.suserid
""",
        "table_schemas": """表名: dws_argothek_oss_login_di
描述: 端页游活跃中间表(分大区不含255)
列名: (数据类型): 描述
  - statis_date (bigint): 统计时间
  - iuserid (string): 用户ID
  - ilogincount (bigint): 登录次数

表名: dws_mgamejp_login_user_activity_di
描述: 平台大盘日活跃表数据
列名: (数据类型): 描述
  - dtstatdate (bigint): 统计日期YYYYMMDD
  - saccounttype (string): 帐号类型:QQ号或者微信
  - suserid (string): 帐号
  - suseridtype (string): 帐号类型:qq wxid playerid 
  - sgamecode (string): 业务 
  - splattype (string): 平台类型(大平台)。枚举值为Android/ iOS，取汇总时取-100 
  - splat (string): 平台(小平台)。备注：写死的-100
  - itimes (bigint): 活跃总次数。备注：该字段表示用户在T日的当日活跃总次数

表名: dim_argothek_gplayerid2qqwxid_df
描述: 全量用户gplayerid转qq或wxid
列名: (数据类型): 描述
  - iuserid (string): 用户id
  - suserid (string): 存储qq/wxid如果微信和qq有绑定关系优先qq
""",
        "knowledge": "峡谷筛选逻辑:\nsgamecode = \"initiatived\" -- 筛选峡谷游戏\nand saccounttype = \"-100\" -- 账号体系，取-100表示汇总\nand suseridtype in (\"qq\", \"wxid\") -- 账号类型，取qq或wxid\nand splattype in (\"-100\", \"PC\") -- 峡谷手游玩家及PC端玩家\nand splat = \"-100\" -- 写死为-100"
    }

    # 运行SQL生成节点
    output = sql_gen_node.run(input_data)

    # 打印输出结果
    print("\n生成的SQL结果：")
    print(output)