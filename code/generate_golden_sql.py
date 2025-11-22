"""
Golden SQL数据生成主脚本
整合问题生成、Schema Linking、SQL生成和验证的完整流程
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Set
from pathlib import Path

from question_generator import QuestionGenerator, create_table_assignments
from schema_linking_generation import SchemaLinkingNode
from sql_generation import SQLGenerationNode
from sql_validator import SQLValidator, batch_validate_sqls


class GoldenSQLGenerator:
    """Golden SQL数据生成器"""

    def __init__(self,
                 true_json_path: str = "code/data/true.json",
                 false_json_path: str = "code/data/false.json",
                 output_path: str = "code/data/generated_golden_sql.json"):
        """
        初始化生成器

        Args:
            true_json_path: true.json文件路径
            false_json_path: false.json文件路径
            output_path: 输出文件路径
        """
        self.true_json_path = true_json_path
        self.false_json_path = false_json_path
        self.output_path = output_path

        # 初始化各个组件
        print("正在初始化组件...")
        self.question_generator = QuestionGenerator()
        self.schema_linking_node = SchemaLinkingNode()
        self.sql_generation_node = SQLGenerationNode()
        self.validator = SQLValidator()

        print("组件初始化完成！\n")

    def extract_all_tables(self) -> Set[str]:
        """从true.json和false.json中提取所有表"""
        tables = set()

        # 从true.json提取
        if os.path.exists(self.true_json_path):
            with open(self.true_json_path, 'r', encoding='utf-8') as f:
                true_data = json.load(f)
                for item in true_data:
                    tables.update(item.get('table_list', []))

        # 从false.json提取
        if os.path.exists(self.false_json_path):
            with open(self.false_json_path, 'r', encoding='utf-8') as f:
                false_data = json.load(f)
                for item in false_data:
                    tables.update(item.get('table_list', []))

        print(f"共提取到 {len(tables)} 张需要覆盖的表")
        return tables

    def generate_questions_for_tables(self, table_list: List[str], target_count: int = 200) -> List[Dict]:
        """
        为表列表生成问题

        Args:
            table_list: 表列表
            target_count: 目标生成数量

        Returns:
            问题列表
        """
        print(f"\n=== 步骤1: 生成问题 ===")
        print(f"目标数量: {target_count}+")

        # 创建表分配策略
        assignments = create_table_assignments(table_list, target_count)

        # 批量生成问题
        questions = self.question_generator.generate_for_table_list(assignments)

        print(f"\n问题生成完成，共生成 {len(questions)} 个问题")
        return questions

    def generate_schema_linking(self, questions: List[Dict]) -> List[Dict]:
        """
        为问题生成Schema Linking

        Args:
            questions: 问题列表

        Returns:
            包含schema_links的问题列表
        """
        print(f"\n=== 步骤2: 生成Schema Linking ===")

        results = []
        total = len(questions)

        for idx, item in enumerate(questions, 1):
            print(f"\n进度: {idx}/{total} - 处理问题...")

            try:
                # 调用Schema Linking节点
                schema_links, table_schemas = self.schema_linking_node.run({
                    'question': item['question'],
                    'table_list': item['table_list'],
                    'knowledge': item.get('knowledge', '')
                })

                # 将结果添加到item中
                item['schema_links'] = schema_links
                item['table_schemas'] = table_schemas

                results.append(item)
                print(f"  ✓ Schema Linking完成，识别到 {len(schema_links)} 个链接")

            except Exception as e:
                print(f"  ✗ Schema Linking失败: {str(e)}")
                # 即使失败也保留这个item，后续可以重试
                item['schema_links'] = []
                item['table_schemas'] = ""
                results.append(item)

        print(f"\nSchema Linking完成，成功处理 {sum(1 for r in results if r.get('schema_links'))} 个问题")
        return results

    def generate_sql(self, questions_with_schema: List[Dict]) -> List[Dict]:
        """
        生成SQL语句

        Args:
            questions_with_schema: 包含schema_links的问题列表

        Returns:
            包含SQL的问题列表
        """
        print(f"\n=== 步骤3: 生成SQL ===")

        results = []
        total = len(questions_with_schema)

        for idx, item in enumerate(questions_with_schema, 1):
            print(f"\n进度: {idx}/{total} - 生成SQL...")

            # 跳过Schema Linking失败的
            if not item.get('schema_links'):
                print(f"  ! 跳过（Schema Linking为空）")
                results.append(item)
                continue

            try:
                # 调用SQL生成节点
                sql_result = self.sql_generation_node.run({
                    'question': item['question'],
                    'table_list': item['table_list'],
                    'schema_links': item['schema_links'],
                    'knowledge': item.get('knowledge', ''),
                    'table_schemas': item.get('table_schemas', '')
                })

                # 将SQL添加到item中
                item['sql'] = sql_result.get('sql', '')

                # 生成sql_id
                if 'sql_id' not in item:
                    item['sql_id'] = f"generated_sql_{idx}"

                # 添加golden_sql标记
                item['golden_sql'] = False  # 生成的数据标记为False，需要验证后才能标记为True

                results.append(item)
                print(f"  ✓ SQL生成完成")

            except Exception as e:
                print(f"  ✗ SQL生成失败: {str(e)}")
                item['sql'] = ""
                results.append(item)

        successful = sum(1 for r in results if r.get('sql'))
        print(f"\nSQL生成完成，成功生成 {successful}/{total} 个SQL语句")
        return results

    def validate_and_filter(self, sql_data: List[Dict], db_config: Dict = None) -> tuple:
        """
        验证SQL并过滤

        Args:
            sql_data: 包含SQL的数据列表
            db_config: 数据库配置（可选）

        Returns:
            (通过验证的数据, 未通过验证的数据)
        """
        print(f"\n=== 步骤4: 验证SQL ===")

        # 只验证有SQL的数据
        data_with_sql = [item for item in sql_data if item.get('sql')]
        print(f"共 {len(data_with_sql)} 条数据需要验证")

        if not data_with_sql:
            print("没有需要验证的数据")
            return [], sql_data

        # 批量验证
        valid_sqls, invalid_sqls = batch_validate_sqls(
            data_with_sql,
            self.validator,
            db_config
        )

        # 标记通过验证的数据为golden_sql
        for item in valid_sqls:
            item['golden_sql'] = True

        return valid_sqls, invalid_sqls

    def save_results(self, valid_data: List[Dict], invalid_data: List[Dict],
                    save_invalid: bool = True) -> None:
        """
        保存生成结果

        Args:
            valid_data: 通过验证的数据
            invalid_data: 未通过验证的数据
            save_invalid: 是否保存未通过验证的数据
        """
        print(f"\n=== 步骤5: 保存结果 ===")

        # 保存通过验证的数据
        output_path = Path(self.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 清理数据：移除验证结果字段
        clean_valid_data = []
        for item in valid_data:
            clean_item = {k: v for k, v in item.items() if k != 'validation_result'}
            # 移除可能的table_schemas字段（这个太大了）
            if 'table_schemas' in clean_item:
                del clean_item['table_schemas']
            if 'schema_links' in clean_item:
                del clean_item['schema_links']
            clean_valid_data.append(clean_item)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(clean_valid_data, f, ensure_ascii=False, indent=2)

        print(f"✓ 成功保存 {len(clean_valid_data)} 条通过验证的数据到: {output_path}")

        # 保存未通过验证的数据（用于分析和改进）
        if save_invalid and invalid_data:
            invalid_path = output_path.parent / "generated_golden_sql_invalid.json"

            clean_invalid_data = []
            for item in invalid_data:
                clean_item = {k: v for k, v in item.items()}
                # 保留验证结果用于分析
                if 'table_schemas' in clean_item:
                    del clean_item['table_schemas']
                if 'schema_links' in clean_item:
                    del clean_item['schema_links']
                clean_invalid_data.append(clean_item)

            with open(invalid_path, 'w', encoding='utf-8') as f:
                json.dump(clean_invalid_data, f, ensure_ascii=False, indent=2)

            print(f"✓ 保存 {len(clean_invalid_data)} 条未通过验证的数据到: {invalid_path}")

    def generate_statistics_report(self, valid_data: List[Dict], invalid_data: List[Dict]) -> Dict:
        """
        生成统计报告

        Args:
            valid_data: 通过验证的数据
            invalid_data: 未通过验证的数据

        Returns:
            统计报告字典
        """
        print(f"\n=== 步骤6: 生成统计报告 ===")

        total = len(valid_data) + len(invalid_data)

        # 复杂度分布
        complexity_dist = {}
        for item in valid_data:
            complexity = item.get('复杂度', '未知')
            complexity_dist[complexity] = complexity_dist.get(complexity, 0) + 1

        # 表覆盖统计
        table_coverage = {}
        for item in valid_data:
            for table in item.get('table_list', []):
                table_coverage[table] = table_coverage.get(table, 0) + 1

        # 生成报告
        report = {
            'generation_time': datetime.now().isoformat(),
            'total_generated': total,
            'valid_count': len(valid_data),
            'invalid_count': len(invalid_data),
            'success_rate': f"{len(valid_data)/total*100:.2f}%" if total > 0 else "0%",
            'complexity_distribution': complexity_dist,
            'table_coverage': {
                'total_tables': len(table_coverage),
                'tables': table_coverage
            }
        }

        # 保存报告
        report_path = Path(self.output_path).parent / "generation_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n统计报告:")
        print(f"  - 总生成数: {total}")
        print(f"  - 通过验证: {len(valid_data)}")
        print(f"  - 未通过: {len(invalid_data)}")
        print(f"  - 成功率: {report['success_rate']}")
        print(f"  - 表覆盖数: {len(table_coverage)}")
        print(f"  - 复杂度分布: {complexity_dist}")
        print(f"\n✓ 报告已保存到: {report_path}")

        return report

    def run(self, target_count: int = 200, db_config: Dict = None) -> Dict:
        """
        运行完整的生成流程

        Args:
            target_count: 目标生成数量
            db_config: 数据库配置（用于验证）

        Returns:
            统计报告
        """
        print("=" * 60)
        print("Golden SQL 数据生成流程启动")
        print("=" * 60)

        # 提取所有需要覆盖的表
        all_tables = self.extract_all_tables()
        table_list = sorted(list(all_tables))

        # 步骤1: 生成问题
        questions = self.generate_questions_for_tables(table_list, target_count)

        # 步骤2: 生成Schema Linking
        questions_with_schema = self.generate_schema_linking(questions)

        # 步骤3: 生成SQL
        sql_data = self.generate_sql(questions_with_schema)

        # 步骤4: 验证SQL
        valid_data, invalid_data = self.validate_and_filter(sql_data, db_config)

        # 步骤5: 保存结果
        self.save_results(valid_data, invalid_data)

        # 步骤6: 生成统计报告
        report = self.generate_statistics_report(valid_data, invalid_data)

        print("\n" + "=" * 60)
        print("Golden SQL 数据生成流程完成！")
        print("=" * 60)

        return report


def main():
    """主函数"""
    # 创建生成器
    generator = GoldenSQLGenerator()

    # 数据库配置（如果需要数据库执行验证）
    # 根据实际情况修改配置
    db_config = {
        'host': '127.0.0.1',
        'user': 'root',
        # 'password': '',  # 根据需要添加
        'db': 'database_name',  # 修改为实际数据库名
        'port': 9030  # StarRocks端口
    }

    # 如果不需要数据库验证，设置为None
    # db_config = None

    # 运行生成流程
    # 注意：首次运行建议只生成少量数据测试，确认流程正常后再生成200+条
    report = generator.run(
        target_count=200,  # 目标生成数量
        db_config=None  # 暂时不进行数据库验证，只做语法检查
    )

    return report


if __name__ == "__main__":
    main()
