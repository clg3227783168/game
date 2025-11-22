"""
向量嵌入数据预处理脚本
将 true.json 中的案例转换为嵌入模型的输入文本
"""

import json
import os

def prepare_text_for_embedding(item: dict) -> str:
    """
    将数据项转换为嵌入模型的输入文本

    Args:
        item: true.json中的单条记录

    Returns:
        拼接后的文本，适合输入嵌入模型
    """
    parts = []

    # 问题描述（主要内容）
    parts.append(f"{item['question']}")

    # 业务知识
    parts.append(f"{item['knowledge']}")

    # 表名列表
    if item.get('table_list'):
        tables = ', '.join(item['table_list'])
        parts.append(f"{tables}")

    return ' '.join(parts)


def generate_embedding_inputs(true_json_path: str, output_path: str = None):
    """
    从 true.json 生成嵌入模型的输入数据

    Args:
        true_json_path: true.json 文件路径
        output_path: 输出文件路径（可选）

    Returns:
        包含 sql_id 和对应文本的列表
    """
    with open(true_json_path, 'r', encoding='utf-8') as f:
        cases = json.load(f)

    embedding_inputs = []

    for case in cases:
        embedding_inputs.append({"sql_id": case["sql_id"], "text": prepare_text_for_embedding(case)})

    # 保存到文件
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(embedding_inputs, f, ensure_ascii=False, indent=2)
        print(f"已保存 {len(embedding_inputs)} 条嵌入输入到 {output_path}")

    return embedding_inputs


def print_sample_output(embedding_inputs: list, num_samples: int = 2):
    """打印样本输出，便于检查"""
    print("\n" + "="*60)
    print("样本输出（可直接复制到嵌入模型）")
    print("="*60)

    for item in enumerate(embedding_inputs[:num_samples]):
        print(f"\n--- {item} ---")
        print()


if __name__ == '__main__':
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 文件路径
    true_json_path = os.path.join(script_dir, 'data', 'true.json')
    output_path = os.path.join(script_dir, 'data', 'embedding_inputs.json')

    # 生成嵌入输入
    embedding_inputs = generate_embedding_inputs(true_json_path, output_path)

    # 打印样本
    print_sample_output(embedding_inputs)

    print(f"\n总共 {len(embedding_inputs)} 条记录需要生成向量")
    print(f"嵌入输入已保存到: {output_path}")
    print("\n推荐嵌入模型:")
    print("  - OpenAI: text-embedding-3-small (维度1536)")
    print("  - 开源中文: bge-large-zh-v1.5 (维度1024)")
    print("\n下一步:")
    print("1. 使用嵌入模型对每条 text 生成向量")
    print("2. 将向量保存到 data/embeddings.json")
    print("3. 格式: {'embeddings': [{'sql_id': 'xxx', 'vector': [...]}]}")
