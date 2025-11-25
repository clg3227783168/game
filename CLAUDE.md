# CLAUDE.md

**一定要在.venv/虚拟环境中执行python命令**

## 项目概述

这是一个基于 LangChain/LangGraph 的 Text-to-SQL 项目，用于从自然语言问题生成 SQL 查询语句。专注于游戏数据分析场景。

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
