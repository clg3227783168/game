"""
SQL案例检索器 - 基于向量相似度从true.json中检索相似案例
用于Few-Shot Learning的案例检索
"""

import json
import os
import numpy as np
from typing import List, Dict, Optional
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

        embeddings_list = data.get('embeddings', [])

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
        query_vector: List[float],
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
        if self.faiss_index is None:
            raise RuntimeError("向量索引未初始化，请先加载embeddings.json")

        # 准备查询向量
        query = np.array([query_vector], dtype='float32')
        faiss.normalize_L2(query)

        # 执行检索（获取更多结果用于表名过滤）
        search_k = top_k
        distances, indices = self.faiss_index.search(query, min(search_k, len(self.sql_ids)))

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

    def retrieve_similar_cases(
        self,
        question: str,
        table_list: List[str] = None,
        top_k: int = 3,
        query_vector: List[float] = None
    ) -> List[Dict]:
        """
        检索最相似的案例（兼容旧接口）

        Args:
            question: 目标问题
            table_list: 涉及的表列表
            top_k: 返回前k个最相似的案例
            query_vector: 查询向量（如果提供则使用向量检索）

        Returns:
            相似案例列表
        """
        return self.retrieve_by_vector(query_vector, top_k, table_list)


    def prepare_query_text(self, question: str, knowledge: str = None, table_list: List[str] = None) -> str:
        """
        准备查询文本，用于生成查询向量

        Args:
            question: 问题描述
            knowledge: 业务知识
            table_list: 表名列表

        Returns:
            拼接后的查询文本
        """
        parts = [f"问题: {question}"]

        if knowledge:
            parts.append(f"业务知识: {knowledge}")

        if table_list:
            tables = ', '.join(table_list)
            parts.append(f"涉及表: {tables}")

        return '\n'.join(parts)

    def is_vector_search_available(self) -> bool:
        """检查向量检索是否可用"""
        return self.faiss_index is not None


# 使用示例
if __name__ == '__main__':

    retriever = SQLCaseRetriever()

    if retriever.is_vector_search_available():
        print("向量检索已启用")
        print("\n使用方法:")
        print("1. 使用 retriever.prepare_query_text() 准备查询文本")
        print("2. 将文本发送到嵌入模型获取向量")
        print("3. 调用 retriever.retrieve_by_vector(query_vector, top_k=3)")

        # 演示文本相似度检索
        test_question = "统计2024年各玩法的参与人数"
        results = retriever.retrieve_similar_cases(
            question=test_question,
            table_list=['dws_jordass_mode_roundrecord_di'],
            top_k=3
        )

        print(f"\n查询: {test_question}")
        print(f"找到 {len(results)} 条相似案例:")
        for i, case in enumerate(results, 1):
            print(f"  {i}. [{case['sql_id']}] {case['question'][:50]}...")
