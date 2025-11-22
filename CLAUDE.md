# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个基于 LangChain/LangGraph 的 Text-to-SQL 项目，用于从自然语言问题生成 SQL 查询语句。项目专注于游戏数据分析场景。

核心功能包括：
- Schema Linking：从自然语言问题中识别需要使用的表、列、JOIN 关系和具体值
- SQL 执行：通过 PyMySQL 连接数据库执行 SQL 并验证结果
- 根据Schema Linking的结果生成aql语句 未实现

## 项目结构

```
game/
├── CLAUDE.md                    # 项目说明文档
├── .gitignore                   # Git 忽略配置
├── dataset_exe_result.json      # 数据集执行结果
├── 代码使用说明.md               # 代码使用说明
├── 方案介绍.md                   # 方案介绍文档
└── code/                        # 主要代码目录
    ├── main.py                  # 主程序入口
    ├── schema_linking.py        # Schema Linking 节点实现
    ├── sql_exe.py               # SQL 执行模块
    ├── base_agent.py            # 基础 Agent 类
    ├── divide_true_false.py     # 数据集划分脚本
    ├── requirements.txt         # Python 依赖
    ├── llm_backend/             # LLM 后端模块
    │   ├── __init__.py
    │   └── getllm.py            # LLM 获取接口
    ├── json/                    # JSON 数据文件
    │   ├── final_dataset.json   # 完整数据集
    │   ├── true.json            # 已有正确SQL语句的参考数据
    │   ├── false.json           # 需要生成SQL语句的题目数据
    │   ├── schema.json          # 数据库 Schema 信息
    │   ├── insert_sql.json      # 插入 SQL 语句
    │   └── common_knowledge.md  # 通用知识文档
    ├── sql_file/                # SQL 文件目录
    │   ├── create_table.sql     # 建表语句
    │   └── drop_table.sql       # 删表语句
```

###  Schema Linking 节点 (`schema_linking.py`)
负责从自然语言问题中识别数据库 schema 元素。

**使用方式：**
```python
from schema_linking import SchemaLinkingNode

node = SchemaLinkingNode()
result = node.run({
    'question': '统计开服至今使用频率最高的用户昵称',
    'table_list': ['dwd_argothek_playerlogin_hi'],
    'knowledge': '昵称筛选逻辑：nickname按照#进行划分，取#前半部分的作为nickname'
})
```

### 数据格式
数据集中每条记录包含：
```json
{
    "sql_id": "sql_28",
    "question": "统计各个玩法上线首周留存情况...",
    "sql": "SELECT ...",
    "复杂度": "中等",
    "table_list": ["dws_jordass_mode_roundrecord_di"],
    "knowledge": "说明：广域战场...",
    "golden_sql": true
}
```
