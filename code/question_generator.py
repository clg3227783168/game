"""
问题生成模块
基于数据库表结构和业务知识，使用LLM自动生成自然语言问题
"""

import json
import random
from typing import Dict, List, Tuple
from langchain_core.messages import HumanMessage, SystemMessage
from llm_backend.getllm import get_claude_llm


class QuestionGenerator:
    """问题生成器类"""

    def __init__(self, schema_path: str = "code/data/schema.json",
                 knowledge_path: str = "code/data/common_knowledge.md",
                 true_json_path: str = "code/data/true.json"):
        """
        初始化问题生成器

        Args:
            schema_path: 数据库schema文件路径
            knowledge_path: 通用业务知识文件路径
            true_json_path: 参考样本文件路径
        """
        self.schema = self._load_json(schema_path)
        self.knowledge = self._load_knowledge(knowledge_path)
        self.examples = self._load_json(true_json_path)
        self.llm = get_claude_llm()

    def _load_json(self, path: str) -> dict:
        """加载JSON文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_knowledge(self, path: str) -> str:
        """加载业务知识文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def _get_table_schema(self, table_name: str) -> Dict:
        """获取指定表的schema信息"""
        for table in self.schema:
            if table['table_name'] == table_name:
                return table
        return None

    def _get_example_questions(self, num_examples: int = 3) -> List[Dict]:
        """从true.json中随机抽取示例问题"""
        return random.sample(self.examples, min(num_examples, len(self.examples)))

    def _build_prompt(self, table_name: str, table_schema: Dict,
                      complexity: str, num_questions: int = 1) -> str:
        """
        构建生成问题的Prompt

        Args:
            table_name: 表名
            table_schema: 表结构信息
            complexity: 复杂度（简单/中等/复杂）
            num_questions: 生成问题数量
        """
        # 获取示例问题
        examples = self._get_example_questions(3)
        examples_text = "\n\n".join([
            f"示例{i+1}：\n问题：{ex['question']}\n复杂度：{ex['复杂度']}\n业务知识：{ex.get('knowledge', '无')}"
            for i, ex in enumerate(examples)
        ])

        # 格式化表结构
        columns_text = "\n".join([
            f"  - {col['column_name']}: {col.get('column_comment', '无注释')}"
            for col in table_schema.get('columns', [])
        ])

        # 判断表的数据层级
        table_layer = "未知"
        if table_name.startswith('dwd_'):
            table_layer = "DWD明细数据层"
        elif table_name.startswith('dws_'):
            table_layer = "DWS汇总数据层"
        elif table_name.startswith('dim_'):
            table_layer = "DIM维度数据层"

        # 复杂度指导
        complexity_guide = {
            "简单": "使用基础的 SELECT、WHERE、GROUP BY 查询，不超过2张表的JOIN",
            "中等": "使用多表JOIN（2-3张表）、子查询、窗口函数或UNION，有一定的业务逻辑复杂度",
            "复杂": "使用CTE、多层嵌套子查询、复杂窗口函数、多个UNION ALL、或涉及4张以上表的JOIN"
        }

        prompt = f"""你是一个专业的游戏数据分析师，需要为Text-to-SQL系统生成高质量的训练数据。

## 任务目标
基于给定的数据库表结构，生成 {num_questions} 个符合业务场景的自然语言问题。

## 表信息
- 表名：{table_name}
- 数据层级：{table_layer}
- 表注释：{table_schema.get('table_comment', '无')}
- 字段列表：
{columns_text}

## 业务知识参考
{self.knowledge[:500]}...

## 参考示例
以下是一些高质量的问题示例：
{examples_text}

## 要求
1. **复杂度**：生成的问题必须是【{complexity}】复杂度
   - {complexity_guide[complexity]}

2. **业务场景**：问题应该符合游戏数据分析的真实业务场景，例如：
   - 用户活跃分析（DAU、留存、回流等）
   - 玩法参与分析（对局统计、模式偏好等）
   - 付费分析（充值、消费统计等）
   - 用户分类（新老用户、高低活等）
   - 时间维度分析（日/周/月统计、趋势分析等）

3. **问题风格**：
   - 使用自然、专业的表达方式
   - 明确指定时间范围（如果适用）
   - 包含具体的统计指标或分析维度
   - 避免过于简单或模糊的问题

4. **业务知识**：
   - 如果问题涉及特殊的业务规则或字段含义，需要提供相应的业务知识说明
   - 例如：特殊字段的计算方式、业务术语的定义等

## 输出格式
请严格按照以下JSON格式输出（只输出JSON，不要有其他内容）：

```json
[
  {{
    "question": "具体的自然语言问题",
    "knowledge": "相关的业务知识说明（如果不需要特殊说明可以为空字符串）",
    "complexity": "{complexity}",
    "table_list": ["{table_name}"]
  }}
]
```

如果需要生成多个问题，请在数组中包含多个对象。
"""
        return prompt

    def generate_questions(self, table_name: str, complexity: str,
                          num_questions: int = 1) -> List[Dict]:
        """
        为指定表生成问题

        Args:
            table_name: 表名
            complexity: 复杂度（简单/中等/复杂）
            num_questions: 生成问题数量

        Returns:
            生成的问题列表
        """
        # 获取表结构
        table_schema = self._get_table_schema(table_name)
        if not table_schema:
            print(f"警告：未找到表 {table_name} 的schema信息")
            return []

        # 构建prompt
        prompt = self._build_prompt(table_name, table_schema, complexity, num_questions)

        # 调用LLM生成
        try:
            messages = [
                SystemMessage(content="你是一个专业的数据分析师和Text-to-SQL训练数据生成专家。"),
                HumanMessage(content=prompt)
            ]

            response = self.llm.invoke(messages)
            result_text = response.content

            # 解析JSON结果
            # 去除可能的markdown标记
            result_text = result_text.strip()
            if result_text.startswith('```json'):
                result_text = result_text[7:]
            if result_text.startswith('```'):
                result_text = result_text[3:]
            if result_text.endswith('```'):
                result_text = result_text[:-3]
            result_text = result_text.strip()

            questions = json.loads(result_text)

            # 确保返回的是列表
            if not isinstance(questions, list):
                questions = [questions]

            # 添加复杂度字段（使用中文键）
            for q in questions:
                q['复杂度'] = complexity
                # 确保table_list存在
                if 'table_list' not in q:
                    q['table_list'] = [table_name]

            print(f"成功为表 {table_name} 生成 {len(questions)} 个{complexity}问题")
            return questions

        except Exception as e:
            print(f"生成问题时出错（表：{table_name}）: {str(e)}")
            return []

    def generate_for_table_list(self, table_assignments: Dict[str, List[Tuple[str, int]]]) -> List[Dict]:
        """
        批量生成问题

        Args:
            table_assignments: 表分配字典
                格式: {table_name: [(complexity, num_questions), ...]}
                例如: {"dws_jordass_login_di": [("简单", 2), ("中等", 3)]}

        Returns:
            所有生成的问题列表
        """
        all_questions = []
        total_tables = len(table_assignments)

        for idx, (table_name, assignments) in enumerate(table_assignments.items(), 1):
            print(f"\n进度: {idx}/{total_tables} - 处理表: {table_name}")

            for complexity, num_questions in assignments:
                questions = self.generate_questions(table_name, complexity, num_questions)
                all_questions.extend(questions)

        return all_questions


def create_table_assignments(table_list: List[str], target_count: int = 200) -> Dict[str, List[Tuple[str, int]]]:
    """
    创建表分配策略

    Args:
        table_list: 需要覆盖的表列表
        target_count: 目标生成问题数量

    Returns:
        表分配字典
    """
    # 按层级分类
    dws_tables = [t for t in table_list if t.startswith('dws_')]
    dwd_tables = [t for t in table_list if t.startswith('dwd_')]
    dim_tables = [t for t in table_list if t.startswith('dim_')]

    assignments = {}

    # DWS层：每张表4-5条（业务核心）
    for table in dws_tables:
        # 简单1条、中等2-3条、复杂1条
        assignments[table] = [
            ("简单", 1),
            ("中等", random.choice([2, 3])),
            ("复杂", 1)
        ]

    # DWD层：每张表2-3条
    for table in dwd_tables:
        # 简单1条、中等1-2条
        num_medium = random.choice([1, 2])
        assignments[table] = [
            ("简单", 1),
            ("中等", num_medium)
        ]

    # DIM层：每张表1-2条
    for table in dim_tables:
        # 主要是简单和中等
        if random.random() < 0.5:
            assignments[table] = [("简单", 1)]
        else:
            assignments[table] = [("简单", 1), ("中等", 1)]

    # 计算总数
    total = sum(sum(num for _, num in tasks) for tasks in assignments.values())
    print(f"预计生成问题数: {total}")
    print(f"  - DWS层表({len(dws_tables)}张): ~{len(dws_tables) * 4}条")
    print(f"  - DWD层表({len(dwd_tables)}张): ~{len(dwd_tables) * 2.5}条")
    print(f"  - DIM层表({len(dim_tables)}张): ~{len(dim_tables) * 1.5}条")

    return assignments


if __name__ == "__main__":
    # 测试代码
    generator = QuestionGenerator()

    # 测试单表生成
    test_table = "dws_jordass_login_di"
    print(f"测试为表 {test_table} 生成问题...\n")

    questions = generator.generate_questions(test_table, "中等", 2)

    print("\n生成结果:")
    print(json.dumps(questions, ensure_ascii=False, indent=2))
