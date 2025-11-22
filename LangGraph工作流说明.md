# LangGraph SQL 生成工作流说明

## 工作流架构

本项目使用 LangGraph 构建了一个完整的 Text-to-SQL 生成和验证流程，支持自动重试机制。

### 流程图

```
┌─────────────────┐
│   开始          │
│  (Initial State)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Schema Linking │◄────────┐
│  (识别表、列、值) │          │
└────────┬────────┘          │
         │                   │
         ▼                   │
┌─────────────────┐          │
│ SQL Generation  │          │
│  (生成SQL语句)   │          │
└────────┬────────┘          │
         │                   │
         ▼                   │
┌─────────────────┐          │
│  SQL Checker    │          │
│  (验证SQL正确性) │          │
└────────┬────────┘          │
         │                   │
         ▼                   │
    ┌────────┐               │
    │ 检查结果│               │
    └───┬────┘               │
        │                    │
    ┌───▼───┐                │
    │ 正确？ │                │
    └───┬───┘                │
        │                    │
    ┌───▼────┬──────┐        │
    │  YES   │  NO  │        │
    └───┬────┴──┬───┘        │
        │       │            │
        │   ┌───▼─────┐      │
        │   │达到最大  │      │
        │   │重试次数？│      │
        │   └───┬─────┘      │
        │       │            │
        │   ┌───▼────┬───┐   │
        │   │  YES   │NO │   │
        │   └───┬────┴─┬─┘   │
        │       │      │     │
        │       │      └─────┘
        │       │    (重试)
        ▼       ▼
    ┌─────────────┐
    │   结束 (END) │
    └─────────────┘
```

## 核心组件

### 1. 状态定义 (GraphState)

```python
class GraphState(TypedDict):
    # 输入字段
    sql_id: str              # SQL ID
    question: str            # 自然语言问题
    table_list: list         # 涉及的表列表
    knowledge: str           # 业务知识
    复杂度: str              # 问题复杂度

    # 中间结果
    schema_links: list       # Schema Linking 结果
    sql: str                 # 生成的 SQL

    # 控制字段
    retry_count: int         # 当前重试次数
    max_retries: int         # 最大重试次数（默认3次）
    is_valid: bool           # SQL 是否有效
    error_message: str       # 错误消息
```

### 2. 节点 (Nodes)

#### 节点1: Schema Linking
- **功能**: 从自然语言问题中识别需要使用的表、列、JOIN关系和具体值
- **输入**: question, table_list, knowledge
- **输出**: schema_links（列表）
- **实现**: `schema_linking_node(state: GraphState) -> GraphState`

#### 节点2: SQL Generation
- **功能**: 基于 Schema Links 和参考案例生成 SQL 语句
- **输入**: question, table_list, knowledge, schema_links
- **输出**: sql（字符串）
- **实现**: `sql_generation_node(state: GraphState) -> GraphState`

#### 节点3: SQL Checker
- **功能**: 使用 LangChain 的 SQLDatabaseToolkit 检查 SQL 正确性
- **输入**: sql
- **输出**: is_valid（布尔值）, error_message
- **实现**: `sql_checker_node(state: GraphState) -> GraphState`

### 3. 边 (Edges)

#### 普通边
- `schema_linking` → `sql_generation`
- `sql_generation` → `sql_checker`

#### 条件边
- `sql_checker` → 根据检查结果决定：
  - 如果 `is_valid == True`: → `END` (结束)
  - 如果 `is_valid == False` 且 `retry_count < max_retries`: → `schema_linking` (重试)
  - 如果 `is_valid == False` 且 `retry_count >= max_retries`: → `END` (结束)

**条件判断函数**: `should_retry(state: GraphState) -> Literal["end", "retry"]`

## 使用方法

### 单条数据处理

```python
from main import single_pipeline

# 输入数据
input_data = {
    "sql_id": "sql_001",
    "question": "统计2024年1月活跃用户数",
    "复杂度": "简单",
    "table_list": ["dws_mgamejp_login_user_activity_di"],
    "knowledge": "活跃用户定义：当日有登录行为的用户"
}

# 运行流程
result = single_pipeline(input_data)

# 结果包含
# {
#     'sql_id': 'sql_001',
#     'sql': '生成的SQL语句',
#     'retry_count': 1,  # 实际重试次数
#     'is_valid': True   # 是否验证通过
# }
```

### 批量处理

```python
from main import batch_pipeline

batch_pipeline(
    input_file='data/false.json',
    output_file='output/generated_sqls.json'
)
```

## 重试机制

1. **最大重试次数**: 默认为 3 次
2. **重试触发条件**: SQL 检查失败（`is_valid == False`）
3. **重试策略**: 回到 Schema Linking 节点重新开始
4. **重试计数**: 每次执行 SQL Checker 后 `retry_count += 1`
5. **停止条件**:
   - SQL 验证通过
   - 达到最大重试次数

## 优势

1. **自动重试**: 如果生成的 SQL 不正确，自动重试而不是直接失败
2. **状态管理**: LangGraph 自动管理整个流程的状态
3. **可视化**: 清晰的节点和边定义，易于理解和维护
4. **可扩展**: 可以轻松添加新的节点或修改流程
5. **错误处理**: 完善的错误处理和日志输出

## 执行示例输出

```
████████████████████████████████████████████████████████████████████████████████
开始处理: sql_001
问题: 统计2024年1月活跃用户数...
████████████████████████████████████████████████████████████████████████████████

================================================================================
步骤1: Schema Linking (尝试 1/3)
================================================================================

识别到 3 个 schema links
  1. dws_mgamejp_login_user_activity_di.vplayerid
  2. dws_mgamejp_login_user_activity_di.dtstatdate
  3. 2024-01

================================================================================
步骤2: SQL Generation
================================================================================

生成的 SQL:
--------------------------------------------------------------------------------
SELECT COUNT(DISTINCT vplayerid) FROM ...
--------------------------------------------------------------------------------

================================================================================
步骤3: SQL Checker
================================================================================

✅ SQL 检查通过！

✅ SQL 验证通过，流程结束

████████████████████████████████████████████████████████████████████████████████
处理完成
████████████████████████████████████████████████████████████████████████████████
SQL ID: sql_001
重试次数: 1
验证结果: ✅ 通过
...
```

## 文件说明

- `main.py`: 主程序文件，包含 LangGraph 工作流定义
- `schema_linking_generation.py`: Schema Linking 节点实现
- `sql_generation.py`: SQL Generation 节点实现
- `sql_checker.py`: SQL Checker 节点实现
- `base_agent.py`: 基础 Agent 类
- `llm_backend/getllm.py`: LLM 接口

## 依赖要求

```
pymysql
langchain
langgraph          # LangGraph 核心库
langchain-community
langchain-anthropic
```

安装依赖：
```bash
pip install -r requirements.txt
```
