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

    def run(self, input_data: Dict[str, Any]) -> List[str]:
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
        print(table_schemas)

        # 调用 LLM 并处理
        response = self.chain.invoke(prompt_input)
        print(response)
        print("''''''''''")
        validated = SchemaValidator(self.table_columns)
        return (validated.validate_and_filter(self._parse_response(response)), table_schemas)

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
        "sql_id": "sql_28",
        "question": "统计各个玩法上线首周留存情况\n输出：玩法、上线首周首次玩的日期、第几天留存（0,1,2...7)、玩法留存用户数\n\n各玩法首周上线日期：\n\"广域战场\": \"20240723\",\n\"消灭战\": \"20230804\",\n\"幻想混战\": \"20241115\",\n\"荒野传说\": \"20240903\",\n\"策略载具\": \"20241010\",\n\"炎夏混战\": \"20240625\",\n\"单人装备\": \"20240517\",\n\"交叉堡垒\": \"20240412\"",
        "sql": "select  a.itype,\n        a.dtstatdate,\n        datediff(b.dtstatdate,a.dtstatdate) as idaynum,\n        count(distinct a.vplayerid)           as iusernum\nfrom (                      \n    select\n        itype,\n        min(dtstatdate) as dtstatdate,\n        vplayerid\n    from  (\n        select '广域战场'      as itype,\n                min(dtstatdate) as dtstatdate,\n                vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',6)\n        and submodename = '广域战场模式'\n        group by vplayerid\n\n        union all\n        select '消灭战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',6)\n        and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by vplayerid\n\n        union all\n        select '幻想混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',6)\n        and modename='创意创作间' and submodename='幻想混战'\n        group by vplayerid\n\n        union all\n        select '荒野传说', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',6)\n        and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by vplayerid\n\n        union all\n        select '策略载具', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',6)\n        and modename='休闲模式' and submodename like '%策略载具%'\n        group by vplayerid\n\n        union all\n        select '炎夏混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',6)\n        and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by vplayerid\n\n        union all\n        select '单人装备', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',6)\n        and modename='组队竞技' and submodename like '%单人装备%'\n        group by vplayerid\n\n        union all\n        select '交叉堡垒', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',6)\n        and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by vplayerid\n    ) t\n    group by itype, vplayerid\n) a\nleft join (\n        select '广域战场' as itype, dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',13)\n          and submodename = '广域战场模式'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '消灭战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',13)\n          and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '幻想混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',13)\n          and modename='创意创作间' and submodename='幻想混战'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '荒野传说', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',13)\n          and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by dtstatdate, vplayerid\n\n        union all\n        select '策略载具', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',13)\n          and modename='休闲模式' and submodename like '%策略载具%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '炎夏混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',13)\n          and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '单人装备', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',13)\n          and modename='组队竞技' and submodename like '%单人装备%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '交叉堡垒', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',13)\n          and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by dtstatdate, vplayerid\n) b\n  on  a.itype      = b.itype\nand  a.vplayerid    = b.vplayerid\nwhere datediff(b.dtstatdate,a.dtstatdate) between 0 and 7\ngroup by a.itype, a.dtstatdate, datediff(b.dtstatdate,a.dtstatdate);\n",
        "复杂度": "中等",
        "table_list": [
            "dws_jordass_mode_roundrecord_di"
        ],
        "knowledge": "说明：\n广域战场 （2024/7/23）submodename= '广域战场模式'，\n消灭战（2023/8/4） modename='组队竞技' and submodename like '%消灭战模式%'，\n幻想混战（2024/11/15）modename='创意创作间' and submodename='幻想混战'，\n荒野传说（2024-09-03）modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')，\n策略载具（2024-10-10）modename='休闲模式' and submodename like '%策略载具%'，\n炎夏混战（2024-06-25）modename='创意创作间' and submodename like '%炎夏混战%'，\n单人装备（2024.5.17）modename='组队竞技' and submodename like '%单人装备%'，\n交叉堡垒（2024.4.12） modename='组队竞技' and submodename like '%交叉堡垒%'\n\n第几天留存：0表示当天参与、1表示当天参与在第2天也参与、2表示当天参与在第3天也参与，依此类推",
    }

    node = SchemaLinkingNode()
    schema_links, _ = node.run(test_data)

    print("\nSchema Links:")
    for i, link in enumerate(schema_links, 1):
        print(f"{i}. {link}")
    print(f"\n总共识别 {len(schema_links)} 个")
