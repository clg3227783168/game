"""
向量检索测试脚本
测试向量检索功能并与文本相似度对比
"""

import os
import sys
from sql_case_retrive import SQLCaseRetriever
from inference import BGEEmbedding


def test_vector_retrieval():
    """测试向量检索功能"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    true_json_path = os.path.join(script_dir, 'data', 'true.json')
    embeddings_path = os.path.join(script_dir, 'data', 'embeddings.json')

    # 检查文件
    if not os.path.exists(embeddings_path):
        print("错误: embeddings.json 不存在")
        print("请先运行:")
        print("  1. python prepare_embeddings.py")
        print("  2. python inference.py")
        return

    # 初始化检索器
    print("初始化向量检索器...")
    retriever = SQLCaseRetriever(true_json_path, embeddings_path)

    if not retriever.is_vector_search_available():
        print("向量检索不可用")
        return

    print(f"✓ 向量索引加载成功")
    print(f"  向量数量: {len(retriever.sql_ids)}")
    print(f"  向量维度: {retriever.dimension}")
    print()

    # 初始化嵌入模型
    print("加载BGE嵌入模型...")
    embedder = BGEEmbedding()
    print()

    # 测试案例
    test_cases = [
        {
            "question": "统计2024年各个玩法的参与用户数和留存情况",
            "table_list": ["dws_jordass_mode_roundrecord_di"],
            "knowledge": "需要统计不同玩法的用户参与情况"
        },
        {
            "question": "查询2024年12月玩家登录次数",
            "table_list": ["dws_jordass_login_di"],
            "knowledge": ""
        },
        {
            "question": "统计按钮点击事件的用户行为",
            "table_list": ["dws_jordass_buttonpress_pre_di"],
            "knowledge": "需要分析用户点击确认的流程"
        }
    ]

    for i, test_case in enumerate(test_cases, 1):
        print("=" * 80)
        print(f"测试案例 {i}")
        print("=" * 80)
        print(f"问题: {test_case['question']}")
        print(f"表名: {', '.join(test_case['table_list'])}")
        print()

        # 准备查询
        query_text = retriever.prepare_query_text(
            question=test_case['question'],
            knowledge=test_case.get('knowledge'),
            table_list=test_case['table_list']
        )

        print("查询文本:")
        print(query_text)
        print()

        # 向量检索
        print("--- 向量检索结果 ---")
        query_vector = embedder.encode([query_text])[0].tolist()
        vector_results = retriever.retrieve_by_vector(
            query_vector=query_vector,
            top_k=3,
            table_filter=test_case['table_list']
        )

        for j, case in enumerate(vector_results, 1):
            print(f"\n{j}. [{case['sql_id']}] 复杂度: {case.get('复杂度', 'N/A')}")
            print(f"   问题: {case['question'][:80]}...")
            if case.get('knowledge'):
                print(f"   知识: {case['knowledge'][:80]}...")

        # 文本相似度检索（对比）
        print("\n--- 文本相似度检索结果（对比）---")
        text_results = retriever._retrieve_by_text_similarity(
            question=test_case['question'],
            table_list=test_case['table_list'],
            top_k=3
        )

        for j, case in enumerate(text_results, 1):
            print(f"\n{j}. [{case['sql_id']}] 复杂度: {case.get('复杂度', 'N/A')}")
            print(f"   问题: {case['question'][:80]}...")

        print("\n")

    print("=" * 80)
    print("测试完成！")
    print("=" * 80)


def simple_search_demo():
    """简单检索演示"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    true_json_path = os.path.join(script_dir, 'data', 'true.json')

    # 初始化
    retriever = SQLCaseRetriever(true_json_path)
    if not retriever.is_vector_search_available():
        print("向量检索不可用，请先生成embeddings.json")
        return

    embedder = BGEEmbedding()

    # 交互式查询
    print("\n向量检索演示（输入'quit'退出）")
    print("-" * 80)

    while True:
        question = input("\n请输入查询问题: ").strip()
        if question.lower() in ['quit', 'exit', 'q']:
            break

        if not question:
            continue

        # 准备查询
        query_text = retriever.prepare_query_text(question)
        query_vector = embedder.encode([query_text])[0].tolist()

        # 检索
        results = retriever.retrieve_by_vector(query_vector, top_k=3)

        print(f"\n找到 {len(results)} 条相似案例:")
        for i, case in enumerate(results, 1):
            print(f"\n{i}. [{case['sql_id']}]")
            print(f"   问题: {case['question'][:100]}...")
            print(f"   表: {', '.join(case.get('table_list', []))}")
            print(f"   复杂度: {case.get('复杂度', 'N/A')}")

    print("\n再见！")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='向量检索测试')
    parser.add_argument('--demo', action='store_true', help='运行交互式演示')
    args = parser.parse_args()

    if args.demo:
        simple_search_demo()
    else:
        test_vector_retrieval()
