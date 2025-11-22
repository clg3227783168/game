"""
使用 BGE-large-zh-v1.5 模型生成向量嵌入
支持GPU加速推理
"""

import json
import os
from typing import List
import torch
from transformers import AutoTokenizer, AutoModel
import numpy as np
from tqdm import tqdm


class BGEEmbedding:
    """BGE嵌入模型封装"""

    def __init__(self, model_name: str = "BAAI/bge-large-zh-v1.5", device: str = None):
        """
        初始化BGE模型

        Args:
            model_name: 模型名称或路径
            device: 设备 ('cuda', 'cpu', 或 None 自动检测)
        """
        if device is None:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = torch.device(device)

        print(f"使用设备: {self.device}")
        print(f"加载模型: {model_name}")

        # 加载分词器和模型
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()

        print("模型加载完成")

    def encode(
        self,
        texts: List[str],
        batch_size: int = 8,
        max_length: int = 512,
        normalize: bool = True
    ) -> np.ndarray:
        """
        编码文本为向量

        Args:
            texts: 文本列表
            batch_size: 批处理大小
            max_length: 最大文本长度
            normalize: 是否归一化向量

        Returns:
            向量数组 (n_texts, embedding_dim)
        """
        all_embeddings = []

        with torch.no_grad():
            for i in tqdm(range(0, len(texts), batch_size), desc="编码向量"):
                batch_texts = texts[i:i + batch_size]

                # 分词
                encoded_input = self.tokenizer(
                    batch_texts,
                    padding=True,
                    truncation=True,
                    max_length=max_length,
                    return_tensors='pt'
                )

                # 移动到设备
                encoded_input = {k: v.to(self.device) for k, v in encoded_input.items()}

                # 前向传播
                model_output = self.model(**encoded_input)

                # 使用CLS token作为句子嵌入
                embeddings = model_output[0][:, 0]  # (batch_size, hidden_size)

                # 归一化
                if normalize:
                    embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)

                all_embeddings.append(embeddings.cpu().numpy())

        # 拼接所有批次
        all_embeddings = np.vstack(all_embeddings)
        return all_embeddings


def generate_embeddings(
    input_path: str,
    output_path: str,
    model_name: str = "BAAI/bge-large-zh-v1.5",
    batch_size: int = 8,
    device: str = None
):
    """
    生成嵌入向量

    Args:
        input_path: 输入JSON文件路径 (embedding_inputs.json)
        output_path: 输出JSON文件路径 (embeddings.json)
        model_name: 模型名称
        batch_size: 批处理大小
        device: 设备
    """
    # 加载输入数据
    print(f"加载输入数据: {input_path}")
    with open(input_path, 'r', encoding='utf-8') as f:
        input_data = json.load(f)

    print(f"共 {len(input_data)} 条记录")

    # 提取文本和元数据
    texts = [item['text'] for item in input_data]
    sql_ids = [item['sql_id'] for item in input_data]

    # 初始化模型
    embedder = BGEEmbedding(model_name=model_name, device=device)

    # 生成嵌入
    print("开始生成向量...")
    embeddings = embedder.encode(texts, batch_size=batch_size)
    print(f"生成完成，向量维度: {embeddings.shape}")

    # 构建输出数据
    output_data = {
        "model": model_name,
        "dimension": int(embeddings.shape[1]),
        "count": len(embeddings),
        "embeddings": []
    }

    for sql_id, vector in zip(sql_ids, embeddings):
        output_data["embeddings"].append({
            "sql_id": sql_id,
            "vector": vector.tolist()
        })

    # 保存结果
    print(f"保存结果到: {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"✓ 向量生成完成！")
    print(f"  模型: {model_name}")
    print(f"  向量数量: {len(embeddings)}")
    print(f"  向量维度: {embeddings.shape[1]}")
    print(f"  输出文件: {output_path}")


if __name__ == '__main__':
    # 设置路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, 'data', 'embedding_inputs.json')
    output_path = os.path.join(script_dir, 'data', 'embeddings.json')

    # 检查输入文件
    if not os.path.exists(input_path):
        print(f"错误: 输入文件不存在 {input_path}")
        print("请先运行: python prepare_embeddings.py")
        exit(1)

    # 配置参数
    MODEL_NAME = "BAAI/bge-large-zh-v1.5"  # 使用BGE中文大模型
    BATCH_SIZE = 8  # 根据GPU显存调整，显存大可以增加到16或32
    DEVICE = None  # None表示自动检测，可指定'cuda'或'cpu'

    # 显示GPU信息
    if torch.cuda.is_available():
        print(f"检测到GPU: {torch.cuda.get_device_name(0)}")
        print(f"显存: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.2f} GB")
    else:
        print("未检测到GPU，将使用CPU（速度较慢）")

    # 生成嵌入
    generate_embeddings(
        input_path=input_path,
        output_path=output_path,
        model_name=MODEL_NAME,
        batch_size=BATCH_SIZE,
        device=DEVICE
    )
