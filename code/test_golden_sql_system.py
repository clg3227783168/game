"""
Golden SQL 生成系统快速测试脚本
用于验证各模块是否正常工作（不实际调用LLM）
"""

import json
import os
import sys
from pathlib import Path


def test_imports():
    """测试模块导入"""
    print("=" * 60)
    print("测试 1: 模块导入检查")
    print("=" * 60)

    try:
        from question_generator import QuestionGenerator, create_table_assignments
        print("✓ question_generator 导入成功")
    except Exception as e:
        print(f"✗ question_generator 导入失败: {e}")
        return False

    try:
        from sql_validator import SQLValidator
        print("✓ sql_validator 导入成功")
    except Exception as e:
        print(f"✗ sql_validator 导入失败: {e}")
        return False

    try:
        from generate_golden_sql import GoldenSQLGenerator
        print("✓ generate_golden_sql 导入成功")
    except Exception as e:
        print(f"✗ generate_golden_sql 导入失败: {e}")
        return False

    try:
        from schema_linking_generation import SchemaLinkingNode
        print("✓ schema_linking_generation 导入成功")
    except Exception as e:
        print(f"✗ schema_linking_generation 导入失败: {e}")
        return False

    try:
        from sql_generation import SQLGenerationNode
        print("✓ sql_generation 导入成功")
    except Exception as e:
        print(f"✗ sql_generation 导入失败: {e}")
        return False

    print("\n所有模块导入成功！\n")
    return True


def test_data_files():
    """测试数据文件是否存在"""
    print("=" * 60)
    print("测试 2: 数据文件检查")
    print("=" * 60)

    required_files = {
        'schema.json': 'code/data/schema.json',
        'true.json': 'code/data/true.json',
        'false.json': 'code/data/false.json',
        'common_knowledge.md': 'code/data/common_knowledge.md'
    }

    all_exist = True
    for name, path in required_files.items():
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"✓ {name} 存在 ({size:,} bytes)")
        else:
            print(f"✗ {name} 不存在: {path}")
            all_exist = False

    if all_exist:
        print("\n所有数据文件检查通过！\n")
    else:
        print("\n警告：部分数据文件缺失\n")

    return all_exist


def test_table_extraction():
    """测试表提取功能"""
    print("=" * 60)
    print("测试 3: 表提取功能")
    print("=" * 60)

    try:
        from generate_golden_sql import GoldenSQLGenerator

        generator = GoldenSQLGenerator()
        tables = generator.extract_all_tables()

        print(f"✓ 成功提取 {len(tables)} 张表")
        print(f"  示例表: {list(tables)[:5]}")
        print()
        return True

    except Exception as e:
        print(f"✗ 表提取失败: {e}\n")
        return False


def test_table_assignment():
    """测试表分配策略"""
    print("=" * 60)
    print("测试 4: 表分配策略")
    print("=" * 60)

    try:
        from question_generator import create_table_assignments

        # 测试用的表列表
        test_tables = [
            'dws_jordass_login_di',
            'dwd_jordass_payrespond_hi',
            'dim_jordass_playerid2suserid_nf'
        ]

        assignments = create_table_assignments(test_tables, target_count=10)

        total_questions = sum(sum(num for _, num in tasks) for tasks in assignments.values())

        print(f"✓ 表分配策略生成成功")
        print(f"  分配 {len(assignments)} 张表")
        print(f"  预计生成 {total_questions} 个问题")
        print(f"  示例分配:")
        for table, tasks in list(assignments.items())[:2]:
            print(f"    - {table}: {tasks}")
        print()
        return True

    except Exception as e:
        print(f"✗ 表分配策略失败: {e}\n")
        return False


def test_sql_validator():
    """测试SQL验证器"""
    print("=" * 60)
    print("测试 5: SQL 验证器")
    print("=" * 60)

    try:
        from sql_validator import SQLValidator

        validator = SQLValidator()

        # 测试SQL
        test_sql = "SELECT vplayerid, dtstatdate FROM dws_jordass_login_di WHERE dtstatdate >= '2024-01-01'"

        result = validator.validate_syntax(
            test_sql,
            expected_tables=['dws_jordass_login_di']
        )

        if result['valid']:
            print(f"✓ SQL 语法验证通过")
            print(f"  提取的表: {result['extracted_tables']}")
        else:
            print(f"✗ SQL 语法验证失败")
            print(f"  错误: {result['errors']}")

        if result.get('warnings'):
            print(f"  警告: {result['warnings']}")

        print()
        return result['valid']

    except Exception as e:
        print(f"✗ SQL 验证器测试失败: {e}\n")
        return False


def test_schema_data():
    """测试schema数据结构"""
    print("=" * 60)
    print("测试 6: Schema 数据结构")
    print("=" * 60)

    try:
        with open('code/data/schema.json', 'r', encoding='utf-8') as f:
            schema = json.load(f)

        print(f"✓ Schema 文件解析成功")
        print(f"  总表数: {len(schema)}")

        # 检查数据结构
        if schema and len(schema) > 0:
            sample_table = schema[0]
            required_fields = ['table_name', 'columns']

            missing = [f for f in required_fields if f not in sample_table]
            if missing:
                print(f"  警告：缺少字段 {missing}")
            else:
                print(f"  ✓ 数据结构正确")
                print(f"  示例表: {sample_table['table_name']}")
                print(f"  列数: {len(sample_table.get('columns', []))}")

        print()
        return True

    except Exception as e:
        print(f"✗ Schema 数据测试失败: {e}\n")
        return False


def test_true_json():
    """测试true.json数据"""
    print("=" * 60)
    print("测试 7: True.json 参考数据")
    print("=" * 60)

    try:
        with open('code/data/true.json', 'r', encoding='utf-8') as f:
            true_data = json.load(f)

        print(f"✓ True.json 解析成功")
        print(f"  参考案例数: {len(true_data)}")

        if true_data and len(true_data) > 0:
            sample = true_data[0]
            required_fields = ['question', 'sql', 'table_list']

            missing = [f for f in required_fields if f not in sample]
            if missing:
                print(f"  警告：缺少字段 {missing}")
            else:
                print(f"  ✓ 数据结构正确")

            # 统计复杂度分布
            complexity_dist = {}
            for item in true_data:
                c = item.get('复杂度', '未知')
                complexity_dist[c] = complexity_dist.get(c, 0) + 1

            print(f"  复杂度分布: {complexity_dist}")

        print()
        return True

    except Exception as e:
        print(f"✗ True.json 测试失败: {e}\n")
        return False


def main():
    """运行所有测试"""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 10 + "Golden SQL 生成系统快速测试" + " " * 18 + "║")
    print("╚" + "=" * 58 + "╝")
    print()

    tests = [
        ("模块导入", test_imports),
        ("数据文件", test_data_files),
        ("表提取", test_table_extraction),
        ("表分配策略", test_table_assignment),
        ("SQL验证器", test_sql_validator),
        ("Schema数据", test_schema_data),
        ("参考数据", test_true_json),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"测试 '{name}' 执行时出错: {e}\n")
            results.append((name, False))

    # 汇总结果
    print("=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{status:8} - {name}")

    print()
    print(f"总计: {passed}/{total} 个测试通过")

    if passed == total:
        print("\n🎉 所有测试通过！系统准备就绪。")
        print("\n下一步:")
        print("  1. 查看 'Golden_SQL生成使用指南.md' 了解详细使用方法")
        print("  2. 运行 'python generate_golden_sql.py' 开始生成数据")
        return 0
    else:
        print(f"\n⚠️  有 {total - passed} 个测试失败，请检查配置")
        return 1


if __name__ == "__main__":
    sys.exit(main())
