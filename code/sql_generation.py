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


class SQLGenerationResult(BaseModel):
    """SQL生成结果的结构化输出模型"""
    sql: str = Field(
        description="生成的SQL查询语句，必须是完整可执行的SQL"
    )

SQL_GENERATION_PROMPT = """你是一个专业的查询类SQL生成专家。

**系统固有知识：**
{common_knowledge}

**任务**：根据自然语言问题、Schema Linking结果、表结构信息和参考案例，生成准确的SQL查询语句。

**问题：**
{question}

**业务知识：**
{knowledge}

**Schema Linking结果：**
{schema_links}

**表结构信息：**
{table_schemas}

**相似案例：**
{reference_case}

---

**要求：**
1. 仔细分析问题需求和业务知识
2. 使用Schema Linkings识别出的表和列，参考对应的表结构信息
3. 参考相似案例的SQL写法和模式
4. 确保SQL语法正确，逻辑清晰
5. 直接生成完整可执行的SQL语句

**【重要】输出格式要求（必须严格遵守）：**
- 只输出完整的SQL语句，不要任何解释、说明或其他文字
- 使用以下格式包裹SQL语句：
```sql
[你的SQL语句]
```
- 如果不使用代码块，也可以直接输出SQL语句，但不能有任何额外的文本

现在请生成SQL：
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
                'reference_case'
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

        formatted_case = self._format_reference_case(self.sql_id_to_case.get(self.false2true_map.get(sql_id, ''), ''))

        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': question,
            'knowledge': knowledge if knowledge else '无',
            'schema_links': schema_links,
            'table_schemas': table_schemas,
            'reference_case': formatted_case
        }

        # 调用LLM生成SQL（文本输出）
        print("\n[2/2] 调用LLM生成SQL...")

        # 打印完整的prompt内容
        print("\n" + "="*80)
        print("完整 Prompt:")
        print("="*80)
        formatted_prompt = self.prompt.format(**prompt_input)
        print(formatted_prompt)
        print("="*80 + "\n")

        response = self.chain.invoke(prompt_input)

        # 获取响应文本
        response_text = response.content if hasattr(response, 'content') else str(response)

        # 解析提取SQL
        sql = self._extract_sql(response_text)

        if not sql:
            print(f"\n[警告] 无法从响应中提取SQL语句")
            print(f"原始响应: {response_text[:200]}...")
            return {
                "sql": "",
                "explanation": "无法从LLM响应中提取有效的SQL语句",
                "notes": "LLM可能返回了非SQL内容",
                "raw_response": response_text
            }

        # 返回成功结果
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
            pattern = f'({keyword}.*?)(?:$|\n\n|(?=\n[^-\s]))'
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
        "question": "统计以下时间对应玩法id下的不同对局时长人数分布\n'20240717'-'20240723' 1310822585431254784'20241116'-'20241122' 1282371711108385024'20241123'-'20241129' 1287652322611036928'20240301'-'20240307' 1294211358396518400'20240607''20240613' 1297394991875754752\n对局时长按照 [0-10min)，[10-30min)，[30min-1h)，[1h,2h)，[2h,3h)，[3h,4h)， [4h,5h)， [5h,6h)，[6h,7h)，[7h,8h)，[8h,9h)，[9h,10h)，[10h,∞) 输出字段：玩法id、对局时长、人数",
        "sql_id": "sql_58",
        "table_list": [
            "dws_jordass_matchlog_stat_di"
        ],
        "schema_links": "dim_vplayerid_vies_df.suserid dim_vplayerid_vies_df.dtstatdate dim_vplayerid_vies_df.itag dws_mgamejp_login_user_activity_di.suserid dws_mgamejp_login_user_activity_di.sgamecode dws_mgamejp_login_user_activity_di.dtstatdate dws_mgamejp_login_user_activity_di.ionlinetime dws_mgamejp_login_user_activity_di.saccounttype dws_mgamejp_login_user_activity_di.suseridtype dws_mgamejp_login_user_activity_di.splattype dws_mgamejp_login_user_activity_di.splat dim_vplayerid_vies_df.suserid=dws_mgamejp_login_user_activity_di.suserid 20250724 其他 20250530 initiatived jordass esports allianceforce strategy playzone su -100 qq wxid",
        "knowledge": "对局时长分布统计",
        "table_schemas": """表名: dim_vplayerid_vies_df
描述: 全量玩家重合竞品表
列:
  - dtstatdate (string): 统计日期
  - vgameappid (string): 系统
  - vplayerid (string): gplayerid
  - suserid (string): suserid
  - suserid_type (string): suserid类型
  - itag (string): 用户分层标签
  - is_reg (bigint): 是否当日新进
  - is_actv (bigint): 是否当日活跃
  - is_neibu (bigint): 是否内部玩家
  - is_lowfps (bigint): 是否新进低帧率
  - cbitmap (string): 活跃位图最左最新活跃
  - gender (string): 性别
  - province (string): 省份
  - city (string): 城市
  - city_level (string): 城市等级
  - iregdate (string): 注册日期
  - iregdate_agamek6 (string): 注册日期_端游
  - lastdate (string): 最后活跃日期
  - lastdate_agamek6 (string): 最后活跃日期_端游
  - lastdays (bigint): 当日距离最后活跃的天数
  - lastdays_agamek6 (bigint): 当日距离最后活跃的天数_未注册为-1_当日活跃为1_上日活跃为2
  - lastdays_fps (bigint): FPS手游
  - lastdays_vie1 (bigint): 战役先锋手游 esports
  - lastdays_vie2 (bigint): 突出重围 mobile_live
  - lastdays_vie3 (bigint): 枪火争锋手游 allianceforce
  - lastdays_vie4 (bigint): 豪杰对决 strategy
  - lastdays_vie5 (bigint): 砺刃使者 jordass
  - lastdays_vie6 (bigint): 天弈 su 
  - lastdays_vie7 (bigint): 勇士召唤手游 playzone 
  - lastdays_vie8 (bigint): 峡谷手游活跃 initiatived
  - lastdays_vie9 (bigint): 峡谷全量活跃 initiatived
  - lastdays_vie10 (bigint): 峡谷端游活跃 initiatived
  - lastdays_vie11 (bigint): 预留
  - lastdays_vie12 (bigint): 预留
  - lastdays_vie13 (bigint): 预留
  - lastdays_vie14 (bigint): 预留
  - lastdays_vie15 (bigint): 预留
  - vtemp1 (string): 预留
  - vtemp2 (string): 预留
  - vtemp3 (string): 预留
  - itemp1 (bigint): 预留
  - itemp2 (bigint): 预留
  - itemp3 (bigint): 预留"""
    }

    # 运行SQL生成节点
    output = sql_gen_node.run(input_data)

    # 打印输出结果
    print("\n生成的SQL结果：")
    print(output)