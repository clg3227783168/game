"""
SQL案例检索器 - 基于向量相似度从true.json中检索相似案例
用于Few-Shot Learning的案例检索
"""

import json
import os
import numpy as np
from typing import List, Dict, Optional
from pathlib import Path
import faiss

class SQLCaseRetriever:
    """SQL案例检索器 - 基于向量相似度检索相似案例"""

    def __init__(self):
        """
        初始化检索器

        Args:
            true_cases_path: true.json文件路径
            embeddings_path: embeddings.json文件路径（向量数据）
        """
        with open(str(Path(__file__).parent / "data/true.json"), 'r', encoding='utf-8') as f:
            self.cases = json.load(f)
        self.case_index = {case['sql_id']: case for case in self.cases}

        # 向量检索相关
        self.embeddings = None
        self.faiss_index = None
        self.sql_ids = []
        self.dimension = None

        """加载预计算的向量嵌入"""
        with open(str(Path(__file__).parent / "data/embeddings.json"), 'r', encoding='utf-8') as f:
            data = json.load(f)

        embeddings_list = data.get('embeddings')

        # 构建FAISS索引
        self.sql_ids = [item['sql_id'] for item in embeddings_list]
        vectors = [item['vector'] for item in embeddings_list]
        self.embeddings = np.array(vectors, dtype='float32')
        self.dimension = self.embeddings.shape[1]

        # 使用L2距离的FAISS索引
        self.faiss_index = faiss.IndexFlatIP(self.dimension)  # 内积相似度
        # 归一化向量用于余弦相似度
        faiss.normalize_L2(self.embeddings)
        self.faiss_index.add(self.embeddings)

        print(f"已加载 {len(self.sql_ids)} 条向量，维度: {self.dimension}")

    def retrieve_by_vector(
        self,
        sql_id: str,
        top_k: int = 3
    ) -> List[Dict]:
        """
        使用查询向量检索最相似的案例

        Args:
            query_vector: 查询问题的向量表示
            top_k: 返回前k个最相似的案例
        Returns:
            相似案例列表
        """
        # 根据sql_id找到对应的向量
        try:
            query_idx = self.sql_ids.index(sql_id)
            query_vector = self.embeddings[query_idx]
        except ValueError:
            raise ValueError(f"找不到 sql_id '{sql_id}' 对应的向量")

        # 准备查询向量
        query = np.array([query_vector], dtype='float32')
        faiss.normalize_L2(query)

        distances, indices = self.faiss_index.search(query, top_k)

        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1:
                continue

            sql_id = self.sql_ids[idx]
            case = self.case_index.get(sql_id)

            if case is None:
                continue

            results.append({
                'case': case,
                'score': float(dist),
                'sql_id': sql_id
            })

            if len(results) >= top_k:
                break

        return [item['case'] for item in results]

# 使用示例
if __name__ == '__main__':
    from pathlib import Path

    retriever = SQLCaseRetriever()

    # 读取 false.json 文件
    false_json_path = Path(__file__).parent / "data/false.json"
    with open(false_json_path, 'r', encoding='utf-8') as f:
        false_cases = json.load(f)

    print(f"开始为 {len(false_cases)} 条 false.json 数据检索相似案例...")

    # 存储结果
    results_mapping = {}

    # 为每个 false.json 中的数据找到最相似的2条数据的 sql_id
    for idx, case in enumerate(false_cases, 1):
        sql_id = case['sql_id']

        try:
            # 检索最相似的案例 (top_k=3，因为可能包含自己，所以多取一个)
            similar_cases = retriever.retrieve_by_vector(
                sql_id=sql_id,
                top_k=3
            )

            # 提取 sql_id，过滤掉自己（如果存在）
            similar_sql_ids = []
            for similar_case in similar_cases:
                if similar_case['sql_id'] != sql_id:
                    similar_sql_ids.append(similar_case['sql_id'])
                if len(similar_sql_ids) >= 2:
                    break

            results_mapping[sql_id] = similar_sql_ids[:2]

            if idx % 10 == 0:
                print(f"已处理 {idx}/{len(false_cases)} 条数据...")

        except ValueError as e:
            print(f"警告: {sql_id} 无法找到对应的向量: {e}")
            results_mapping[sql_id] = []

    # 输出结果到 false2true.json
    output_path = Path(__file__).parent / "data/false2true.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results_mapping, f, ensure_ascii=False, indent=2)

    print(f"\n完成！结果已保存到: {output_path}")
    print(f"共处理 {len(results_mapping)} 条数据")
