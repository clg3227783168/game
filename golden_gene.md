Golden SQL 数据生成方案

     目标

     生成 200+条 格式与 code/data/true.json 一致的 golden_sql 
     数据，覆盖 76 张表

     数据生成流程（6个步骤）

     步骤1：表分配策略生成

     创建 code/generate_golden_sql.py 主脚本，实现：
     - 读取 76 张目标表清单
     - 按层级和重要性分配生成任务：
       - DWS层（17张）：每张生成 4-5 条（业务核心）
       - DWD层（43张）：每张生成 2-3 条
       - DIM层（16张）：每张生成 1-2 条
     - 确保总量达到 200+ 条
     - 按复杂度分配：简单30%、中等50%、复杂20%

     步骤2：问题自动生成

     使用 LLM (getllm.py) 为每张表生成业务问题：
     - 输入：表名 + schema结构 + common_knowledge.md
     - 输出：自然语言问题 + 业务知识说明 + 复杂度标签
     - Prompt设计：参考 true.json 中的问题风格
     - 多样性保证：统计分析、留存计算、用户分类、时间序列等

     步骤3：Schema Linking 生成

     复用现有 schema_linking_generation.py：
     - 输入：问题 + 表清单 + knowledge
     - 输出：需要的表.列、JOIN关系、具体值

     步骤4：SQL 生成

     使用 sql_generation.py + Few-Shot 检索：
     - 从 true.json 检索相似案例（基于向量或表名）
     - 结合 schema linking 结果
     - 使用 LLM 生成完整 SQL

     步骤5：双重验证

     5.1 语法检查
     - 验证表名/列名是否在 schema.json 中
     - 检查 SQL 语法结构

     5.2 数据库执行验证
     - 使用 sql_exe.py 连接数据库
     - 实际执行 SQL（添加 LIMIT 避免大量数据）
     - 记录执行结果（成功/失败/错误信息）

     步骤6：数据保存与统计

     - 保存通过验证的数据到 code/data/generated_golden_sql.json
     - 生成统计报告：
       - 表覆盖率
       - 复杂度分布
       - 成功率统计

     技术实现

     新建文件

     1. code/generate_golden_sql.py - 主流程控制
     2. code/question_generator.py - 问题生成模块
     3. code/sql_validator.py - SQL验证模块

     复用现有模块

     - llm_backend/getllm.py - LLM调用
     - schema_linking_generation.py - Schema识别
     - sql_generation.py - SQL生成
     - sql_exe.py - 数据库执行
     - sql_case_retrive.py - 案例检索

     数据流

     表清单(76张) 
       → 问题生成(question + knowledge) 
       → Schema Linking 
       → SQL生成(基于Few-Shot) 
       → 验证(语法+执行) 
       → 保存JSON

     预期输出

     - code/data/generated_golden_sql.json（200+条）
     - code/data/generation_report.json（统计报告）
     - 失败案例日志（用于分析改进）