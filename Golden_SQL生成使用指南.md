# Golden SQL 数据生成系统使用指南

## 概述

这是一个基于 LLM 的自动化 Golden SQL 数据生成系统，能够根据数据库表结构自动生成高质量的 Text-to-SQL 训练数据。

## 系统架构

```
┌─────────────────┐
│ 表列表提取      │
│ (true.json +    │
│  false.json)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 问题生成        │ ← question_generator.py
│ (LLM生成)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Schema Linking  │ ← schema_linking_generation.py
│ (表.列识别)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SQL 生成        │ ← sql_generation.py
│ (基于Few-Shot)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SQL 验证        │ ← sql_validator.py
│ (语法+执行)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 保存结果        │
│ generated_      │
│ golden_sql.json │
└─────────────────┘
```

## 核心模块

### 1. question_generator.py - 问题生成模块

**功能**：基于表结构使用 LLM 自动生成业务问题

**主要类**：
- `QuestionGenerator`: 问题生成器
- `create_table_assignments()`: 创建表分配策略

**使用示例**：
```python
from question_generator import QuestionGenerator

generator = QuestionGenerator()

# 为单个表生成问题
questions = generator.generate_questions(
    table_name="dws_jordass_login_di",
    complexity="中等",
    num_questions=2
)

# 批量生成
assignments = {
    "dws_jordass_login_di": [("简单", 1), ("中等", 2)],
    "dwd_jordass_payrespond_hi": [("简单", 1)]
}
all_questions = generator.generate_for_table_list(assignments)
```

**输出格式**：
```json
[
  {
    "question": "统计2024年1月各玩家的登录次数",
    "knowledge": "登录数据存储在dws_jordass_login_di表中",
    "complexity": "简单",
    "table_list": ["dws_jordass_login_di"],
    "复杂度": "简单"
  }
]
```

### 2. sql_validator.py - SQL验证模块

**功能**：验证生成的 SQL 语法和执行正确性

**主要类**：
- `SQLValidator`: SQL验证器

**验证功能**：
1. 语法检查：表名、列名是否存在
2. 数据库执行：实际运行SQL验证可执行性

**使用示例**：
```python
from sql_validator import SQLValidator

validator = SQLValidator()

# 语法验证
result = validator.validate_syntax(
    sql="SELECT vplayerid FROM dws_jordass_login_di",
    expected_tables=["dws_jordass_login_di"]
)

# 完整验证（包含数据库执行）
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'db': 'database_name',
    'port': 9030
}

full_result = validator.validate_sql_full(
    sql="SELECT vplayerid FROM dws_jordass_login_di LIMIT 10",
    db_config=db_config
)
```

### 3. generate_golden_sql.py - 主流程脚本

**功能**：整合所有模块，执行完整的数据生成流程

**主要类**：
- `GoldenSQLGenerator`: 主生成器

**流程步骤**：
1. 从 true.json 和 false.json 提取所有表
2. 为每张表分配生成任务（按层级和复杂度）
3. 使用 LLM 生成问题
4. 执行 Schema Linking
5. 生成 SQL 语句
6. 验证 SQL（语法 + 执行）
7. 保存结果和统计报告

## 快速开始

### 1. 环境准备

确保已安装所有依赖：
```bash
cd code
pip install -r requirements.txt
```

### 2. 配置检查

确认以下文件存在：
- `code/data/schema.json` - 数据库表结构
- `code/data/true.json` - 参考SQL案例（至少15条）
- `code/data/false.json` - 待生成的问题（可选）
- `code/data/common_knowledge.md` - 业务知识

### 3. 测试单个模块

**测试问题生成**：
```bash
cd code
python question_generator.py
```

**测试SQL验证**：
```bash
python sql_validator.py
```

### 4. 运行完整流程

**小规模测试（推荐首次运行）**：

编辑 `generate_golden_sql.py` 的 main 函数：
```python
def main():
    generator = GoldenSQLGenerator()

    # 首次测试：只生成10条数据，不连接数据库
    report = generator.run(
        target_count=10,  # 先测试10条
        db_config=None    # 暂不连接数据库
    )

    return report
```

运行：
```bash
python generate_golden_sql.py
```

**大规模生成（200+条）**：

确认测试成功后，修改配置：
```python
def main():
    generator = GoldenSQLGenerator()

    # 可选：配置数据库以进行执行验证
    db_config = {
        'host': '127.0.0.1',
        'user': 'root',
        # 'password': '',  # 如果需要
        'db': 'your_database_name',
        'port': 9030
    }

    # 生成200+条数据
    report = generator.run(
        target_count=200,
        db_config=db_config  # 或 None（只做语法检查）
    )

    return report
```

运行：
```bash
python generate_golden_sql.py
```

## 输出文件

生成完成后会产生以下文件：

### 1. generated_golden_sql.json
**通过验证的高质量数据**

格式与 true.json 一致：
```json
[
  {
    "sql_id": "generated_sql_1",
    "question": "统计2024年各玩家登录次数",
    "sql": "SELECT vplayerid, COUNT(*) as cnt FROM ...",
    "复杂度": "简单",
    "table_list": ["dws_jordass_login_di"],
    "knowledge": "登录数据按日统计",
    "golden_sql": true
  }
]
```

### 2. generated_golden_sql_invalid.json
**未通过验证的数据**（用于分析和改进）

包含验证失败原因：
```json
[
  {
    "sql_id": "generated_sql_5",
    "question": "...",
    "sql": "SELECT ...",
    "validation_result": {
      "syntax_check": {
        "valid": false,
        "errors": ["未找到的表: xxx"]
      }
    }
  }
]
```

### 3. generation_report.json
**统计报告**

```json
{
  "generation_time": "2025-11-22T10:30:00",
  "total_generated": 220,
  "valid_count": 185,
  "invalid_count": 35,
  "success_rate": "84.09%",
  "complexity_distribution": {
    "简单": 55,
    "中等": 92,
    "复杂": 38
  },
  "table_coverage": {
    "total_tables": 76,
    "tables": {
      "dws_jordass_login_di": 4,
      "dwd_jordass_payrespond_hi": 2,
      ...
    }
  }
}
```

## 高级配置

### 调整生成策略

编辑 `question_generator.py` 中的 `create_table_assignments()` 函数：

```python
def create_table_assignments(table_list: List[str], target_count: int = 200):
    # 自定义各层级表的生成数量
    for table in dws_tables:
        assignments[table] = [
            ("简单", 2),   # DWS层：简单2条
            ("中等", 3),   # 中等3条
            ("复杂", 2)    # 复杂2条
        ]
    # ... 更多自定义
```

### 数据库验证配置

如果要启用数据库执行验证：

```python
db_config = {
    'host': '127.0.0.1',         # 数据库地址
    'user': 'root',              # 用户名
    'password': 'your_password', # 密码（如需要）
    'db': 'your_database',       # 数据库名
    'port': 9030,                # 端口（StarRocks: 9030, MySQL: 3306）
    'charset': 'utf8mb4'         # 字符集（可选）
}
```

### 自定义 Prompt

问题生成的 Prompt 可在 `question_generator.py` 的 `_build_prompt()` 方法中修改。

SQL 生成的 Prompt 在 `sql_generation.py` 的 `SQL_GENERATION_PROMPT` 中修改。

## 常见问题

### Q1: 生成速度慢？
**A**: 这是正常的，因为每个问题都需要调用 LLM 3-4 次（问题生成、Schema Linking、SQL生成）。生成 200 条数据可能需要 1-2 小时。

建议：
- 首次运行生成少量数据测试
- 可以分批生成，多次运行

### Q2: 生成的 SQL 质量不高？
**A**: 可能的原因和解决方案：
1. true.json 中的参考案例太少 → 增加高质量案例
2. schema.json 中的表/列注释不完整 → 补充注释
3. common_knowledge.md 业务知识不足 → 补充业务规则

### Q3: 验证失败率高？
**A**:
1. 如果是语法错误 → 检查 schema.json 是否完整
2. 如果是执行错误 → 可能是数据库连接问题或表不存在
3. 可以先设置 `db_config=None` 只做语法检查

### Q4: 想要生成特定复杂度的数据？
**A**: 修改 `create_table_assignments()` 中的复杂度分配比例。

### Q5: 如何增加某些表的生成数量？
**A**: 在 `create_table_assignments()` 中为特定表增加分配：
```python
# 为重要表增加生成数量
if table == "dws_jordass_login_di":
    assignments[table] = [
        ("简单", 3),
        ("中等", 5),
        ("复杂", 2)
    ]
```

## 性能优化建议

1. **并行生成**：可以修改代码支持多线程/多进程并行调用 LLM
2. **断点续传**：在长时间运行时，建议定期保存中间结果
3. **批量验证**：SQL 验证可以批量进行，提高效率

## 下一步

1. 检查生成的数据质量
2. 对未通过验证的数据进行分析
3. 根据分析结果调整 Prompt 或业务知识
4. 迭代优化，逐步提高成功率
5. 将通过验证的数据合并到 true.json 中

## 技术支持

如遇到问题，请检查：
1. LLM API 是否正常（`code/llm_backend/getllm.py` 中的配置）
2. 数据文件路径是否正确
3. Python 依赖是否完整安装

祝您使用顺利！
