"""
SQL案例检索器 - 基于向量相似度从true.json中检索相似案例
用于Few-Shot Learning的案例检索
"""

import json
import os
import numpy as np
from typing import List, Dict, Optional

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False


class SQLCaseRetriever:
    """SQL案例检索器 - 基于向量相似度检索相似案例"""

    def __init__(self, true_cases_path: str, embeddings_path: str = None):
        """
        初始化检索器

        Args:
            true_cases_path: true.json文件路径
            embeddings_path: embeddings.json文件路径（向量数据）
        """
        self.cases = self._load_cases(true_cases_path)
        self.case_index = {case['sql_id']: case for case in self.cases}

        # 向量检索相关
        self.embeddings = None
        self.faiss_index = None
        self.sql_ids = []
        self.dimension = None

        # 尝试加载向量索引
        if embeddings_path is None:
            embeddings_path = os.path.join(
                os.path.dirname(true_cases_path),
                'embeddings.json'
            )

        if os.path.exists(embeddings_path):
            self._load_embeddings(embeddings_path)

    def _load_cases(self, path: str) -> List[Dict]:
        """加载参考案例"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"参考案例文件不存在: {path}")

        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _load_embeddings(self, path: str):
        """加载预计算的向量嵌入"""
        if not FAISS_AVAILABLE:
            print("警告: FAISS未安装，向量检索不可用，将使用文本相似度")
            return

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        embeddings_list = data.get('embeddings', [])
        if not embeddings_list:
            print("警告: 向量数据为空")
            return

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
        top_k: int = 3,
        table_filter: List[str] = None
    ) -> List[Dict]:
        """
        使用查询向量检索最相似的案例

        Args:
            query_vector: 查询问题的向量表示
            top_k: 返回前k个最相似的案例
            table_filter: 表名过滤列表（可选）

        Returns:
            相似案例列表
        """
        if self.faiss_index is None:
            raise RuntimeError("向量索引未初始化，请先加载embeddings.json")

        # 准备查询向量
        query = np.array([query_vector], dtype='float32')
        faiss.normalize_L2(query)

        # 执行检索（获取更多结果用于表名过滤）
        search_k = top_k * 3 if table_filter else top_k
        distances, indices = self.faiss_index.search(query, min(search_k, len(self.sql_ids)))

        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1:
                continue

            sql_id = self.sql_ids[idx]
            case = self.case_index.get(sql_id)

            if case is None:
                continue

            # 表名过滤
            if table_filter:
                case_tables = set(case.get('table_list', []))
                filter_tables = set(table_filter)
                # 有交集才保留
                if not (case_tables & filter_tables):
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
        # 如果提供了向量且索引可用，使用向量检索
        if query_vector is not None and self.faiss_index is not None:
            return self.retrieve_by_vector(query_vector, top_k, table_list)

        # 回退到文本相似度检索
        return self._retrieve_by_text_similarity(question, table_list, top_k)

    def _retrieve_by_text_similarity(
        self,
        question: str,
        table_list: List[str] = None,
        top_k: int = 3
    ) -> List[Dict]:
        """基于文本相似度的检索（回退方法）"""
        from difflib import SequenceMatcher

        scored_cases = []

        for case in self.cases:
            # 综合评分：问题相似度 + 表匹配度
            question_sim = SequenceMatcher(None, question, case['question']).ratio()

            table_sim = 0.0
            if table_list and case.get('table_list'):
                target_set = set(table_list)
                case_set = set(case['table_list'])
                intersection = target_set & case_set
                union = target_set | case_set
                table_sim = len(intersection) / len(union) if union else 0.0

            # 加权得分
            score = 0.7 * question_sim + 0.3 * table_sim

            scored_cases.append({
                'case': case,
                'score': score,
                'question_sim': question_sim,
                'table_sim': table_sim
            })

        # 按分数排序
        scored_cases.sort(key=lambda x: x['score'], reverse=True)

        # 返回top_k个案例
        return [item['case'] for item in scored_cases[:top_k]]

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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    true_json_path = os.path.join(script_dir, 'data', 'true.json')

    retriever = SQLCaseRetriever(true_json_path)

    if retriever.is_vector_search_available():
        print("向量检索已启用")
        print("\n使用方法:")
        print("1. 使用 retriever.prepare_query_text() 准备查询文本")
        print("2. 将文本发送到嵌入模型获取向量")
        print("3. 调用 retriever.retrieve_by_vector(query_vector, top_k=3)")
    else:
        print("向量检索未启用（缺少 embeddings.json 或 FAISS）")
        print("将使用文本相似度检索")

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
