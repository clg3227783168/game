"""
SQL生成节点 - 基于Schema Linking结果和参考案例生成SQL
利用true.json中的正确SQL案例进行Few-Shot Learning
使用结构化输出确保生成结果的规范性和可解析性
"""

import json
import os
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from llm_backend.getllm import get_claude_llm
from base_agent import BaseAgent
from sql_case_retrive import SQLCaseRetriever
from pathlib import Path


class SQLGenerationResult(BaseModel):
    """SQL生成结果的结构化输出模型"""
    sql: str = Field(
        description="生成的SQL查询语句，必须是完整可执行的SQL"
    )

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

{format_instructions}

现在请按照上述格式生成SQL：
"""


class SQLGenerationNode(BaseAgent):
    """SQL生成节点 - 基于Schema Linking和参考案例生成SQL"""

    def __init__(self):
        """
        初始化SQL生成节点

        Args:
            true_cases_path: true.json路径
            schema_path: schema.json路径
            llm: LangChain LLM实例
        """
        super().__init__()

        # 初始化组件
        self.retriever = SQLCaseRetriever(str(Path(__file__).parent / "data/true.json"),)
        self.llm = get_claude_llm()

        # 创建结构化输出解析器
        self.output_parser = PydanticOutputParser(pydantic_object=SQLGenerationResult)

        # 创建prompt模板（包含格式说明）
        self.prompt = PromptTemplate(
            template=SQL_GENERATION_PROMPT,
            input_variables=[
                'common_knowledge',
                'question',
                'knowledge',
                'schema_links',
                'table_schemas',
                'reference_cases'
            ],
            partial_variables={
                'format_instructions': self.output_parser.get_format_instructions()
            }
        )

        # 创建链：prompt -> llm -> parser
        self.chain = self.prompt | self.llm | self.output_parser

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成SQL语句（结构化输出）

        Args:
            input_data: 输入数据，应包含：
                - question: 问题描述
                - table_list: 表列表
                - schema_links: Schema Linking结果（可选）
                - knowledge: 业务知识（可选）
                - table_schemas: 表结构信息（可选）

        Returns:
            结构化的SQL生成结果字典，包含以下字段：
                - sql: 生成的SQL语句
                - columns_used: 使用的列列表
        """
        question = input_data.get('question')
        table_list = input_data.get('table_list')
        schema_links = input_data.get('schema_links')
        knowledge = input_data.get('knowledge', '')
        table_schemas = input_data.get('table_schemas')

        print(f"\n[SQL生成] 开始生成SQL...")
        print(f"问题: {question[:100]}...")

        prompt_input = {
            'common_knowledge': self.common_knowledge,
            'question': question,
            'knowledge': knowledge if knowledge else '无',
            'schema_links': schema_links,
            'table_schemas': table_schemas,
            'reference_cases': similar_cases
        }

        # 4. 调用LLM生成SQL（结构化输出）
        print("\n[4/4] 调用LLM生成结构化SQL...")
        try:
            result: SQLGenerationResult = self.chain.invoke(prompt_input)

            print(f"\n[完成] SQL生成成功！")

            # 转换为字典返回
            return {
                "sql": result.sql
            }

        except Exception as e:
            print(f"\n[错误] SQL生成失败: {e}")
            import traceback
            traceback.print_exc()

            # 返回空结果
            return {
                "sql": "",
                "explanation": f"生成失败: {str(e)}",
                "notes": "生成过程出现异常"
            }
