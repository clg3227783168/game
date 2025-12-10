import json
import os
from datetime import datetime
from typing import TypedDict, Annotated, Literal, Optional
from langgraph.graph import StateGraph, END
from schema_linking_generation import SchemaLinkingNode
from sql_generation import SQLGenerationNode
from sql_checker import SQLCheckerNode

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
    error_history: list  # 历史错误记录 [(sql, error_msg), ...]


def _init_log_file() -> str:
    """创建批处理日志文件并返回路径"""
    log_dir = os.path.join(os.path.dirname(__file__), "log")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("%Y%m%d_%H%M%S_%f") + ".json"
    log_file_path = os.path.join(log_dir, log_filename)

    with open(log_file_path, 'w', encoding='utf-8') as f:
        json.dump([], f, ensure_ascii=False, indent=2)

    return log_file_path


def _append_graph_state_log(log_file_path: str, graph_state: GraphState) -> None:
    """将 GraphState 追加写入日志文件"""
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            log_content = json.load(f)
        if not isinstance(log_content, list):
            log_content = []
    except Exception:
        log_content = []

    log_content.append(graph_state)

    with open(log_file_path, 'w', encoding='utf-8') as f:
        json.dump(log_content, f, ensure_ascii=False, indent=2)


# 节点1: Schema Linking
def schema_linking_node(state: GraphState) -> GraphState:
    """Schema Linking 节点 - 识别需要的表、列和值"""

    schema_node = SchemaLinkingNode()
    output = schema_node.run({
        'question': state['question'],
        'table_list': state['table_list'],
        'knowledge': state['knowledge']
    })
    return {
        **state,
        'schema_links': output['schema_links'],
        'table_schemas': output['table_schemas']
    }


# 节点2: SQL Generation
def sql_generation_node(state: GraphState) -> GraphState:

    sql_node = SQLGenerationNode()
    result = sql_node.run({
        'sql_id': state['sql_id'],
        'question': state['question'],
        'table_list': state['table_list'],
        'knowledge': state['knowledge'],
        'schema_links': state['schema_links'],
        'table_schemas': state['table_schemas'],
        'error_history': state.get('error_history', [])
    })

    generated_sql = result.get('sql', '')

    return {
        **state,
        'sql': generated_sql
    }


# 节点3: SQL Checker
def sql_checker_node(state: GraphState) -> GraphState:
    """SQL Checker 节点 - 检查 SQL 正确性"""

    checker_node = SQLCheckerNode()
    result = checker_node.run({
        'sql': state['sql']
    })

    is_valid = result.get('is_valid', False)
    message = result.get('message', '')

    # 记录错误历史
    error_history = state.get('error_history', [])
    if not is_valid:
        error_history.append({
            'sql': state['sql'],
            'error_message': message,
            'retry_count': state['retry_count']
        })

    return {
        **state,
        'is_valid': is_valid,
        'error_message': message,
        'retry_count': state['retry_count'] + 1,
        'error_history': error_history
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
        return "end"

    if state['retry_count'] >= state['max_retries']:
        return "end"

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

def single_pipeline(input_dict, log_file_path: Optional[str] = None):
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

    # 初始化状态
    initial_state = {
        'sql_id': input_dict['sql_id'],
        'question': input_dict['question'],
        'table_list': input_dict['table_list'],
        'knowledge': input_dict.get('knowledge', ''),
        '复杂度': input_dict.get('复杂度'),
        'schema_links': [],
        'sql': '',
        'retry_count': 0,
        'max_retries': 3,  # 最多重试3次
        'is_valid': False,
        'error_message': '',
        'error_history': []  # 初始化错误历史
    }

    # 构建并运行工作流
    app = build_workflow()
    final_state = app.invoke(initial_state)

    if log_file_path:
        _append_graph_state_log(log_file_path, final_state)

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
    log_file_path = _init_log_file()
    print(f"日志文件已创建: {log_file_path}")

    # 加载输入数据
    with open(input_file, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    # 加载已有结果（如果存在）
    existing_results = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                # 将已有结果转换为字典，方便查询
                existing_results = {r['sql_id']: r for r in existing_data}
        except Exception as e:
            existing_results = {}

    # 过滤出需要处理的数据（排除已存在的 sql_id）
    todo_items = []
    skipped_count = 0
    for item in dataset:
        sql_id = item.get('sql_id')
        if sql_id in existing_results:
            skipped_count += 1
        else:
            todo_items.append(item)

    print(f"\n需要处理: {len(todo_items)} 条数据")
    print(f"跳过已存在: {skipped_count} 条数据")

    # 处理新数据
    new_results = []
    for i, item in enumerate(todo_items, 1):
        print(f"\n{'='*80}")
        print(f"处理进度: {i}/{len(todo_items)}")
        print(f"{'='*80}")

        result = single_pipeline(item, log_file_path=log_file_path)
        new_results.append(result)
        existing_results[result['sql_id']] = result

        # 每处理一条就保存一次，防止中断导致数据丢失
        all_results = list(existing_results.values())
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)

    # 最终保存
    all_results = list(existing_results.values())
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    # 统计信息
    valid_count = sum(1 for r in all_results if r.get('is_valid', False))


if __name__ == "__main__":
    # 测试单条数据
    # test_data = {
    #     "sql_id": "sql_1",
    #     "question": "统计2025.07.24的手游全量用户且标签为其他，在竞品业务下2025.05.30-2025.07.24的在线时长。\n输出：suserid、sgamecode、ionlinetime\n\n",
    #     "复杂度": "中等",
    #     "table_list": [
    #         "dws_mgamejp_login_user_activity_di",
    #         "dim_vplayerid_vies_df"
    #     ],
    #     "knowledge": "竞品业务：\nsgamecode in (\"initiatived\",\"jordass\",\"esports\",\"allianceforce\",\"strategy\",\"playzone\",\"su\")\nsaccounttype = \"-100\" -- 账号体系，取-100表示汇总\nand suseridtype in (\"qq\",\"wxid\") -- 用户类型\nand splattype = \"-100\" -- 平台类型\nand splat = \"-100\" -- 平台，写死为-100\n"
    # }

    # print("=" * 80)
    # print("LangGraph SQL 生成流程测试")
    # print("=" * 80)

    # 运行测试
    # result = single_pipeline(test_data)

    # print("\n" + "=" * 80)
    # print("最终结果:")
    # print("=" * 80)
    # print(json.dumps(result, ensure_ascii=False, indent=2))

    # 批量处理示例（注释掉，需要时取消注释）
    batch_pipeline(
        input_file='code/data/false.json',
        output_file='code/data/generated_sqls.json'
    )
