import jieba
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# ---------------------------------------------------------
# 1. 定义特征词库 (用于逻辑匹配)
# ---------------------------------------------------------
LOGIC_KEYWORDS = {
    "retention": ["留存", "次留", "7留", "回流", "流失"],
    "rank": ["排名", "前1", "top", "最高", "最多", "榜单"],
    "ltv": ["ltv", "arpu", "付费率", "客单价", "充值金额"],
    "complex_filter": ["排除", "剔除", "不包含", "交叉", "同时"],
    "string_ops": ["分割", "截取", "split", "substr", "instr"]
}

def extract_logic_tags(text):
    tags = set()
    for category, keywords in LOGIC_KEYWORDS.items():
        for kw in keywords:
            if kw in text.lower():
                tags.add(category)
                break # 每一类命中一个即可
    return tags

# ---------------------------------------------------------
# 2. 打分函数
# ---------------------------------------------------------
def calculate_similarity(target, example, tfidf_matrix_target, tfidf_matrix_example):
    """
    target/example 结构: {'question': '...', 'table_list': [...], 'knowledge': '...'}
    """
    
    # --- A. 表重合度 (Table Jaccard) [权重 0.6] ---
    target_tables = set(target.get('table_list', []))
    example_tables = set(example.get('table_list', []))
    
    if len(target_tables) == 0: 
        table_score = 0
    else:
        # 只要有交集就给高分，交集越多分越高
        intersection = len(target_tables.intersection(example_tables))
        union = len(target_tables.union(example_tables))
        table_score = intersection / union if union > 0 else 0
        
        # 额外加分：如果 Target 的表被 Example 完全包含，这是极好的例子
        if target_tables.issubset(example_tables) and len(target_tables) > 0:
            table_score += 0.2 

    # --- B. 逻辑特征匹配 (Logic Match) [权重 0.3] ---
    # 结合 question 和 knowledge 一起找关键词
    target_text = target['question'] + str(target.get('knowledge', ''))
    example_text = example['question'] + str(example.get('knowledge', ''))
    
    target_tags = extract_logic_tags(target_text)
    example_tags = extract_logic_tags(example_text)
    
    # 计算 Tag Jaccard
    if len(target_tags) == 0:
        logic_score = 0
    else:
        logic_intersect = len(target_tags.intersection(example_tags))
        logic_union = len(target_tags.union(example_tags))
        logic_score = logic_intersect / logic_union

    # --- C. 文本语义相似度 (TF-IDF Cosine) [权重 0.1] ---
    # 这里直接传入预计算好的 vector 相似度
    text_score = cosine_similarity(tfidf_matrix_target, tfidf_matrix_example)[0][0]

    # --- 最终加权得分 ---
    final_score = (0.6 * table_score) + (0.3 * logic_score) + (0.1 * text_score)
    
    return final_score

# ---------------------------------------------------------
# 3. 主流程工具类
# ---------------------------------------------------------
class FewShotSelector:
    def __init__(self, examples):
        self.examples = examples
        self.corpus = [ex['question'] for ex in examples]
        # 初始化 TF-IDF
        self.vectorizer = TfidfVectorizer(tokenizer=jieba.lcut)
        self.examples_tfidf = self.vectorizer.fit_transform(self.corpus)

    def select_top_k(self, target_question_obj, k=3):
        target_text = target_question_obj['question']
        target_tfidf = self.vectorizer.transform([target_text])
        
        scores = []
        for idx, example in enumerate(self.examples):
            # 获取 Example 的 TF-IDF 向量
            ex_tfidf = self.examples_tfidf[idx]
            
            score = calculate_similarity(
                target_question_obj, 
                example, 
                target_tfidf, 
                ex_tfidf
            )
            scores.append((score, example))
        
        # 按分数降序排列
        scores.sort(key=lambda x: x[0], reverse=True)
        
        # 返回 Top K 的 examples 内容
        return [item[1] for item in scores[:k]]

# ==========================================
# 使用示例
# ==========================================
# 假设 known_examples 是你的 15 条数据 (包含 labeled_output 即 schema linking 的答案)
# 假设 target_data_list 是你的 86 条数据

# selector = FewShotSelector(known_examples)
# 
# for target in target_data_list:
#     best_examples = selector.select_top_k(target, k=3)
#     # 然后把 best_examples 格式化填入 prompt


"""
### 4. 针对常见错误的微调技巧

不同的错误信息，其实暗示了不同的 Prompt 调整方向。你可以在 Python 代码里做一点简单的规则判断，给 `error_feedback` 加点“私货”：

#### 场景 A：列名/表名不存在 (Column not found)
* **错误特征**：`Column 'xyz' cannot be resolved`
* **Prompt 补充指令**：
    > "报错提示列名不存在。请严格检查【表结构信息】，确保只使用表中真实存在的列名，不要使用别名或臆造列名。"

#### 场景 B：聚合错误 (Group By missing)
* **错误特征**：`Expression '...' is neither present in the group by`
* **Prompt 补充指令**：
    > "报错提示 Group By 缺失。请确保 SELECT 中所有非聚合字段都出现在 GROUP BY 子句中。"

#### 场景 C：函数方言错误 (Function not found)
* **错误特征**：`Undefined function: 'DATE_ADD'` (假设是在 Spark/Hive 环境)
* **Prompt 补充指令**：
    > "报错提示函数不存在。请注意当前是 **Hive/Spark SQL** 环境，请检查日期函数是否应该使用 `date_sub` 或 `date_add`，不要使用 MySQL 独有的语法。"

### 总结

1.  **流程**：默认只重试 **SQL 生成阶段**。
2.  **Prompt**：重试时，Prompt = 原 Prompt + **(错误的SQL + 报错信息)**。
3.  **解析**：确保你的 `extract_sql_code` 函数足够健壮，因为重试时的 LLM 往往会道歉（"对不起，我修正了..."），要能从这些废话里再次提取出 SQL。
"""