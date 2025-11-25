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
        with open(str(Path(__file__).parent.parent / "data/true.json"), 'r', encoding='utf-8') as f:
            self.cases = json.load(f)
        self.case_index = {case['sql_id']: case for case in self.cases}

        # 创建 true case 的 sql_id 集合，用于过滤
        self.true_sql_ids = set(case['sql_id'] for case in self.cases)

        # 向量检索相关
        self.embeddings = None
        self.faiss_index = None
        self.sql_ids = []
        self.dimension = None

        """加载预计算的向量嵌入（包含所有 embeddings）"""
        with open(str(Path(__file__).parent.parent / "data/embeddings.json"), 'r', encoding='utf-8') as f:
            data = json.load(f)

        embeddings_list = data.get('embeddings')

        # 构建FAISS索引（包含所有向量）
        self.sql_ids = [item['sql_id'] for item in embeddings_list]
        vectors = [item['vector'] for item in embeddings_list]
        self.embeddings = np.array(vectors, dtype='float32')
        self.dimension = self.embeddings.shape[1]

        # 使用L2距离的FAISS索引
        self.faiss_index = faiss.IndexFlatIP(self.dimension)  # 内积相似度
        # 归一化向量用于余弦相似度
        faiss.normalize_L2(self.embeddings)
        self.faiss_index.add(self.embeddings)

        print(f"已加载 {len(self.sql_ids)} 条向量（包含 true 和 false），维度: {self.dimension}")
        print(f"其中 true case 数量: {len(self.true_sql_ids)}")

    def retrieve_by_vector(
        self,
        sql_id: str,
        top_k: int = 3
    ) -> List[Dict]:
        """
        使用查询向量检索最相似的案例（仅返回 true case）

        Args:
            sql_id: 查询问题的 sql_id
            top_k: 返回前k个最相似的 true case
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

        # 检索足够多的候选结果（确保能找到足够的 true case）
        # 由于 true case 数量较少，需要检索更多候选以确保覆盖所有 true case
        search_k = len(self.sql_ids)  # 检索所有候选，然后过滤出 true case
        distances, indices = self.faiss_index.search(query, search_k)

        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1:
                continue

            candidate_sql_id = self.sql_ids[idx]

            # 跳过查询本身
            if candidate_sql_id == sql_id:
                continue

            # 只保留 true case
            if candidate_sql_id not in self.true_sql_ids:
                continue

            case = self.case_index.get(candidate_sql_id)
            if case is None:
                continue

            results.append({
                'case': case,
                'score': float(dist),
                'sql_id': candidate_sql_id
            })

            # 达到所需数量就停止
            if len(results) >= top_k:
                break

        return [item['case'] for item in results]

# 使用示例
if __name__ == '__main__':
    from pathlib import Path

    retriever = SQLCaseRetriever()

    # 读取 false.json 文件
    false_json_path = Path(__file__).parent.parent / "data/false.json"
    with open(false_json_path, 'r', encoding='utf-8') as f:
        false_cases = json.load(f)

    print(f"开始为 {len(false_cases)} 条 false.json 数据检索相似案例...")

    # 存储结果
    results_mapping = {}

    for idx, case in enumerate(false_cases, 1):
        sql_id = case['sql_id']
        similar_cases = retriever.retrieve_by_vector(
            sql_id=sql_id,
            top_k=3
        )
        similar_sql_ids = []
        for similar_case in similar_cases:
                similar_sql_ids.append(similar_case['sql_id'])

        results_mapping[sql_id] = similar_sql_ids[0]

        if idx % 10 == 0:
            print(f"已处理 {idx}/{len(false_cases)} 条数据...")

    # 输出结果到 false2true.json
    output_path = str(Path(__file__).parent.parent / "data/false2true.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results_mapping, f, ensure_ascii=False, indent=2)

    print(f"\n完成！结果已保存到: {output_path}")
    print(f"共处理 {len(results_mapping)} 条数据")
