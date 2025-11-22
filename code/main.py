import json
import os
from typing import TypedDict, Annotated, Literal
from langgraph.graph import StateGraph, END
from schema_linking_generation import SchemaLinkingNode
from sql_generation import SQLGenerationNode
from sql_checker import SQLCheckerNode
from sql_exe import execute_sql


# 定义 LangGraph 状态
class GraphState(TypedDict):
    """工作流状态定义"""
    # 输入字段
    sql_id: str
    question: str
    table_list: list
    knowledge: str
    复杂度: str

    # 中间结果
    schema_links: list  # Schema Linking 结果
    table_schemas: str
    sql: str  # 生成的 SQL

    # 控制字段
    retry_count: int  # 重试次数
    max_retries: int  # 最大重试次数
    is_valid: bool  # SQL 是否有效
    error_message: str  # 错误消息


# 节点1: Schema Linking
def schema_linking_node(state: GraphState) -> GraphState:
    """Schema Linking 节点 - 识别需要的表、列和值"""
    print("\n" + "=" * 80)
    print(f"步骤1: Schema Linking (尝试 {state['retry_count'] + 1}/{state['max_retries']})")
    print("=" * 80)

    schema_node = SchemaLinkingNode()
    schema_links = schema_node.run({
        'question': state['question'],
        'table_list': state['table_list'],
        'knowledge': state['knowledge']
    })

    print(f"\n识别到 {len(schema_links)} 个 schema links")
    for i, link in enumerate(schema_links[:5], 1):
        print(f"  {i}. {link}")
    if len(schema_links) > 5:
        print(f"  ... 还有 {len(schema_links) - 5} 个")

    return {
        **state,
        'schema_links': schema_links
    }


# 节点2: SQL Generation
def sql_generation_node(state: GraphState) -> GraphState:
    """SQL Generation 节点 - 基于 Schema Links 生成 SQL"""
    print("\n" + "=" * 80)
    print("步骤2: SQL Generation")
    print("=" * 80)

    sql_node = SQLGenerationNode()
    result = sql_node.run({
        'question': state['question'],
        'table_list': state['table_list'],
        'knowledge': state['knowledge'],
        'schema_links': state['schema_links']
    })

    generated_sql = result.get('sql', '')

    print("\n生成的 SQL:")
    print("-" * 80)
    print(generated_sql[:500] + "..." if len(generated_sql) > 500 else generated_sql)
    print("-" * 80)

    return {
        **state,
        'sql': generated_sql
    }


# 节点3: SQL Checker
def sql_checker_node(state: GraphState) -> GraphState:
    """SQL Checker 节点 - 检查 SQL 正确性"""
    print("\n" + "=" * 80)
    print("步骤3: SQL Checker")
    print("=" * 80)

    checker_node = SQLCheckerNode()
    result = checker_node.run({
        'sql': state['sql']
    })

    is_valid = result.get('is_valid', False)
    message = result.get('message', '')

    if is_valid:
        print(f"\n✅ SQL 检查通过！")
    else:
        print(f"\n❌ SQL 检查失败: {message}")
        print(f"当前重试次数: {state['retry_count'] + 1}/{state['max_retries']}")

    return {
        **state,
        'is_valid': is_valid,
        'error_message': message,
        'retry_count': state['retry_count'] + 1
    }


# 条件边: 决定是继续还是重试
def should_retry(state: GraphState) -> Literal["end", "retry"]:
    """
    判断是否需要重试

    Returns:
        "end": SQL 正确或达到最大重试次数，结束流程
        "retry": SQL 错误且未达到最大重试次数，回到 Schema Linking
    """
    if state['is_valid']:
        print("\n✅ SQL 验证通过，流程结束")
        return "end"

    if state['retry_count'] >= state['max_retries']:
        print(f"\n⚠️ 已达到最大重试次数 ({state['max_retries']})，流程结束")
        return "end"

    print(f"\n🔄 准备重试 ({state['retry_count']}/{state['max_retries']})...")
    return "retry"


# 构建 LangGraph 工作流
def build_workflow() -> StateGraph:
    """构建 LangGraph 工作流"""
    workflow = StateGraph(GraphState)

    # 添加节点
    workflow.add_node("schema_linking", schema_linking_node)
    workflow.add_node("sql_generation", sql_generation_node)
    workflow.add_node("sql_checker", sql_checker_node)

    # 设置入口点
    workflow.set_entry_point("schema_linking")

    # 添加边
    workflow.add_edge("schema_linking", "sql_generation")
    workflow.add_edge("sql_generation", "sql_checker")

    # 添加条件边（根据 SQL 检查结果决定是结束还是重试）
    workflow.add_conditional_edges(
        "sql_checker",
        should_retry,
        {
            "end": END,
            "retry": "schema_linking"  # 重试时回到 Schema Linking
        }
    )

    return workflow.compile()


def single_pipeline(input_dict):
    """
    为一条数据生成SQL查询语句（使用 LangGraph）

    Args:
        input_dict: 输入字典，包含以下字段：
            - sql_id: SQL ID
            - question: 自然语言问题
            - sql: 参考SQL（可选）
            - 复杂度: 问题复杂度
            - table_list: 涉及的表列表
            - knowledge: 相关知识

    Returns:
        dict: 输出字典，包含以下字段：
            - sql_id: SQL ID
            - sql: 生成的SQL
            - retry_count: 重试次数
            - is_valid: SQL 是否通过验证
    """
    print("\n" + "█" * 80)
    print(f"开始处理: {input_dict['sql_id']}")
    print(f"问题: {input_dict['question'][:100]}...")
    print("█" * 80)

    # 初始化状态
    initial_state = {
        'sql_id': input_dict['sql_id'],
        'question': input_dict['question'],
        'table_list': input_dict['table_list'],
        'knowledge': input_dict.get('knowledge', ''),
        '复杂度': input_dict.get('复杂度', ''),
        'schema_links': [],
        'sql': '',
        'retry_count': 0,
        'max_retries': 3,  # 最多重试3次
        'is_valid': False,
        'error_message': ''
    }

    # 构建并运行工作流
    app = build_workflow()
    final_state = app.invoke(initial_state)

    # 输出结果
    print("\n" + "█" * 80)
    print("处理完成")
    print("█" * 80)
    print(f"SQL ID: {final_state['sql_id']}")
    print(f"重试次数: {final_state['retry_count']}")
    print(f"验证结果: {'✅ 通过' if final_state['is_valid'] else '❌ 失败'}")
    print("\n最终 SQL:")
    print("-" * 80)
    print(final_state['sql'])
    print("-" * 80)

    return {
        'sql_id': final_state['sql_id'],
        'sql': final_state['sql'],
        'is_valid': final_state['is_valid']
    }


def batch_pipeline(input_file: str, output_file: str):
    """
    批量处理数据集

    Args:
        input_file: 输入 JSON 文件路径（如 false.json）
        output_file: 输出 JSON 文件路径
    """
    # 加载数据
    with open(input_file, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    print(f"\n加载了 {len(dataset)} 条数据")

    results = []
    for i, item in enumerate(dataset, 1):
        print(f"\n{'='*80}")
        print(f"处理进度: {i}/{len(dataset)}")
        print(f"{'='*80}")

        result = single_pipeline(item)
        results.append(result)
    # 保存结果
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*80}")
    print(f"处理完成！结果已保存到: {output_file}")
    print(f"{'='*80}")

    # 统计信息
    valid_count = sum(1 for r in results if r.get('is_valid', False))
    print(f"\n统计信息:")
    print(f"  总数: {len(results)}")
    print(f"  验证通过: {valid_count}")
    print(f"  验证失败: {len(results) - valid_count}")
    print(f"  通过率: {valid_count / len(results) * 100:.2f}%")


if __name__ == "__main__":
    # 测试单条数据
    test_data = {
        "sql_id": "test_001",
        "question": "统计2024年1月到3月每月的活跃用户数",
        "复杂度": "简单",
        "table_list": ["dws_mgamejp_login_user_activity_di"],
        "knowledge": ""
    }

    print("=" * 80)
    print("LangGraph SQL 生成流程测试")
    print("=" * 80)

    # 运行测试
    result = single_pipeline(test_data)

    print("\n" + "=" * 80)
    print("最终结果:")
    print("=" * 80)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 批量处理示例（注释掉，需要时取消注释）
    # batch_pipeline(
    #     input_file='data/false.json',
    #     output_file='output/generated_sqls.json'
    # )
