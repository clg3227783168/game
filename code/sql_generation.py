"""
SQL生成节点 - 基于Schema Linking结果和参考案例生成SQL
利用true.json中的正确SQL案例进行Few-Shot Learning
"""

import json
import os
from typing import List, Dict, Any
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from llm_backend.getllm import get_claude_llm
from base_agent import BaseAgent
from sql_case_retrive import SQLCaseRetriever
import pathlib


SQL_GENERATION_PROMPT = """你是一个专业的查询类SQL生成专家。

**【系统固有知识 - 请牢记】**
{common_knowledge}

---

**任务**：根据自然语言问题、Schema Linking结果和参考案例，生成准确的SQL查询语句。

**问题描述：**
{question}

**业务知识：**
{knowledge}

**Schema Linking结果：**
{schema_links}

**表结构信息：**
{table_schemas}

**参考案例（相似问题的SQL实现）：**
{reference_cases}

---

**要求：**
1. 仔细分析问题需求和参考案例的SQL结构
2. 使用Schema Linking识别出的表和列
3. 参考相似案例的SQL写法和模式
4. 确保SQL语法正确，逻辑清晰
5. 只输出SQL语句，不要包含其他解释

**输出格式：**
```sql
-- 这里写生成的SQL语句
```

现在请生成SQL：
"""


class SQLGenerationNode(BaseAgent):
    """SQL生成节点 - 基于Schema Linking和参考案例生成SQL"""

    def __init__(
        self,
        true_cases_path: str = None,
        schema_path: str = None,
        llm = None
    ):
        """
        初始化SQL生成节点

        Args:
            true_cases_path: true.json路径
            schema_path: schema.json路径
            llm: LangChain LLM实例
        """
        super().__init__()

        # 设置默认路径
        current_dir = os.path.dirname(os.path.abspath(__file__))

        if true_cases_path is None:
            true_cases_path = os.path.join(current_dir, 'data', 'true.json')

        if schema_path is None:
            schema_path = os.path.join(current_dir, 'data', 'schema.json')

        # 初始化组件
        self.retriever = SQLCaseRetriever(true_cases_path)
        self.llm = llm if llm else get_claude_llm()

        # 加载schema信息
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.schema_data = json.load(f)

        # 创建prompt模板
        self.prompt = PromptTemplate(
            template=SQL_GENERATION_PROMPT,
            input_variables=[
                'common_knowledge',
                'question',
                'knowledge',
                'schema_links',
                'table_schemas',
                'reference_cases'
            ]
        )

        # 创建链
        self.chain = self.prompt | self.llm | StrOutputParser()

    def _format_reference_cases(self, cases: List[Dict]) -> str:
        """格式化参考案例"""
        if not cases:
            return "无参考案例"

        formatted = []
        for i, case in enumerate(cases, 1):
            formatted.append(f"\n=== 参考案例 {i} ===")
            formatted.append(f"问题: {case['question'][:150]}...")

            if case.get('knowledge'):
                formatted.append(f"业务知识: {case['knowledge'][:150]}...")

            formatted.append(f"SQL:\n```sql\n{case['sql']}\n```")

        return '\n'.join(formatted)

    def _format_schema_links(self, schema_links: List[str]) -> str:
        """格式化Schema Links"""
        if not schema_links:
            return "无Schema Links"

        # 分类整理
        tables_columns = []
        joins = []
        values = []

        for link in schema_links:
            if '=' in link:
                joins.append(link)
            elif '.' in link:
                tables_columns.append(link)
            else:
                values.append(link)

        result = []

        if tables_columns:
            result.append("需要使用的表和列:")
            for tc in tables_columns:
                result.append(f"  - {tc}")

        if joins:
            result.append("\nJOIN关系:")
            for j in joins:
                result.append(f"  - {j}")

        if values:
            result.append("\n需要的常量值:")
            for v in values:
                result.append(f"  - {v}")

        return '\n'.join(result)

    def _extract_sql_from_response(self, response: str) -> str:
        """从LLM响应中提取SQL语句"""
        # 尝试提取代码块中的SQL
        import re

        # 匹配 ```sql ... ``` 或 ``` ... ```
        pattern = r'```(?:sql)?\s*(.*?)\s*```'
        matches = re.findall(pattern, response, re.DOTALL)

        if matches:
            sql = matches[0].strip()
            # 移除可能的注释行
            lines = sql.split('\n')
            sql_lines = [line for line in lines if not line.strip().startswith('--')]
            return '\n'.join(sql_lines).strip()

        # 如果没有代码块，返回整个响应
        return response.strip()

    def run(self, input_data: Dict[str, Any]) -> Dict[str, str]:
        """
        生成SQL语句

        Args:
            input_data: 输入数据，应包含：
                - question: 问题描述
                - table_list: 表列表
                - schema_links: Schema Linking结果（可选）
                - knowledge: 业务知识（可选）

        Returns:
            包含SQL语句的字典: {"sql": "具体语句内容"}
        """
        question = input_data.get('question')
        table_list = input_data.get('table_list')
        schema_links = input_data.get('schema_links')
        knowledge = input_data.get('knowledge', '')
        table_schemas = input_data.get('table_schemas')

        print(f"\n[SQL生成] 开始生成SQL...")
        print(f"问题: {question[:100]}...")

        # 1. 检索相似案例
        print("\n[1/4] 检索相似案例...")
        similar_cases = self.retriever.retrieve_similar_cases(
            question=question,
            table_list=table_list,
            top_k=3
        )
        print(f"找到 {len(similar_cases)} 个相似案例")

        # 3. 格式化输入
        print("\n[3/4] 构建生成prompt...")
        formatted_cases = self._format_reference_cases(similar_cases)
        formatted_links = self._format_schema_links(schema_links)

        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': question,
            'knowledge': knowledge if knowledge else '无',
            'schema_links': formatted_links,
            'table_schemas': table_schemas,
            'reference_cases': formatted_cases
        }

        # 4. 调用LLM生成SQL
        print("\n[4/4] 调用LLM生成SQL...")
        try:
            response = self.chain.invoke(prompt_input)
            generated_sql = self._extract_sql_from_response(response)

            print(f"\n[完成] SQL生成成功！")
            return {"sql": generated_sql}

        except Exception as e:
            print(f"\n[错误] SQL生成失败: {e}")
            import traceback
            traceback.print_exc()
            return {"sql": ""}
