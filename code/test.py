"""
测试 SchemaLinkingNode 和 SQLGenerationNode
"""
import json
from schema_linking_generation import SchemaLinkingNode
from sql_generation import SQLGenerationNode


def print_section(title):
    """打印分节标题"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def test_nodes(test_data):
    """测试两个节点的执行结果"""

    # 打印测试数据信息
    print_section("测试数据")
    print(f"SQL ID: {test_data['sql_id']}")
    print(f"问题: {test_data['question'][:100]}...")
    print(f"表列表: {test_data['table_list']}")
    print(f"复杂度: {test_data['复杂度']}")

    # 步骤1: Schema Linking
    print_section("步骤 1: Schema Linking")
    schema_linking_node = SchemaLinkingNode()
    schema_links = schema_linking_node.run(test_data)

    print(f"\n识别到的 Schema Links (共 {len(schema_links)} 个):")
    for i, link in enumerate(schema_links, 1):
        print(f"  {i}. {link}")

    # 步骤2: SQL Generation
    print_section("步骤 2: SQL Generation")

    # 构建 SQL 生成节点的输入数据
    sql_input = {
        'question': test_data['question'],
        'table_list': test_data['table_list'],
        'schema_links': schema_links,
        'knowledge': test_data.get('knowledge', '')
    }

    sql_generation_node = SQLGenerationNode()
    result = sql_generation_node.run(sql_input)

    # 打印生成的SQL
    print_section("生成的 SQL 语句")
    print(result.get('sql', ''))

    # 如果有参考SQL，进行对比
    if 'sql' in test_data and test_data['sql']:
        print_section("参考 SQL 语句")
        print(test_data['sql'])

    # 返回结果
    return {
        'sql_id': test_data['sql_id'],
        'schema_links': schema_links,
        'generated_sql': result.get('sql', ''),
        'reference_sql': test_data.get('sql', '')
    }


# 测试数据
test_data = {
    "sql_id": "sql_28",
    "question": "统计各个玩法上线首周留存情况\n输出：玩法、上线首周首次玩的日期、第几天留存（0,1,2...7)、玩法留存用户数\n\n各玩法首周上线日期：\n\"广域战场\": \"20240723\",\n\"消灭战\": \"20230804\",\n\"幻想混战\": \"20241115\",\n\"荒野传说\": \"20240903\",\n\"策略载具\": \"20241010\",\n\"炎夏混战\": \"20240625\",\n\"单人装备\": \"20240517\",\n\"交叉堡垒\": \"20240412\"",
    "sql": "select  a.itype,\n        a.dtstatdate,\n        datediff(b.dtstatdate,a.dtstatdate) as idaynum,\n        count(distinct a.vplayerid)           as iusernum\nfrom (                      \n    select\n        itype,\n        min(dtstatdate) as dtstatdate,\n        vplayerid\n    from  (\n        select '广域战场'      as itype,\n                min(dtstatdate) as dtstatdate,\n                vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',6)\n        and submodename = '广域战场模式'\n        group by vplayerid\n\n        union all\n        select '消灭战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',6)\n        and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by vplayerid\n\n        union all\n        select '幻想混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',6)\n        and modename='创意创作间' and submodename='幻想混战'\n        group by vplayerid\n\n        union all\n        select '荒野传说', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',6)\n        and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by vplayerid\n\n        union all\n        select '策略载具', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',6)\n        and modename='休闲模式' and submodename like '%策略载具%'\n        group by vplayerid\n\n        union all\n        select '炎夏混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',6)\n        and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by vplayerid\n\n        union all\n        select '单人装备', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',6)\n        and modename='组队竞技' and submodename like '%单人装备%'\n        group by vplayerid\n\n        union all\n        select '交叉堡垒', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',6)\n        and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by vplayerid\n    ) t\n    group by itype, vplayerid\n) a\nleft join (\n        select '广域战场' as itype, dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',13)\n          and submodename = '广域战场模式'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '消灭战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',13)\n          and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '幻想混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',13)\n          and modename='创意创作间' and submodename='幻想混战'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '荒野传说', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',13)\n          and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by dtstatdate, vplayerid\n\n        union all\n        select '策略载具', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',13)\n          and modename='休闲模式' and submodename like '%策略载具%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '炎夏混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',13)\n          and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '单人装备', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',13)\n          and modename='组队竞技' and submodename like '%单人装备%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '交叉堡垒', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',13)\n          and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by dtstatdate, vplayerid\n) b\n  on  a.itype      = b.itype\nand  a.vplayerid    = b.vplayerid\nwhere datediff(b.dtstatdate,a.dtstatdate) between 0 and 7\ngroup by a.itype, a.dtstatdate, datediff(b.dtstatdate,a.dtstatdate);\n",
    "复杂度": "中等",
    "table_list": [
        "dws_jordass_mode_roundrecord_di"
    ],
    "knowledge": "说明：\n广域战场 （2024/7/23）submodename= '广域战场模式'，\n消灭战（2023/8/4） modename='组队竞技' and submodename like '%消灭战模式%'，\n幻想混战（2024/11/15）modename='创意创作间' and submodename='幻想混战'，\n荒野传说（2024-09-03）modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')，\n策略载具（2024-10-10）modename='休闲模式' and submodename like '%策略载具%'，\n炎夏混战（2024-06-25）modename='创意创作间' and submodename like '%炎夏混战%'，\n单人装备（2024.5.17）modename='组队竞技' and submodename like '%单人装备%'，\n交叉堡垒（2024.4.12） modename='组队竞技' and submodename like '%交叉堡垒%'\n\n第几天留存：0表示当天参与、1表示当天参与在第2天也参与、2表示当天参与在第3天也参与，依此类推",
}


if __name__ == "__main__":
    print_section("Text-to-SQL 节点测试")

    # 执行测试
    result = test_nodes(test_data)

    # 保存结果（可选）
    print_section("测试完成")
    print(f"测试结果已生成，SQL ID: {result['sql_id']}")
    print(f"Schema Links 数量: {len(result['schema_links'])}")
    print(f"生成的 SQL 长度: {len(result['generated_sql'])} 字符")

    # 可选：保存结果到文件
    output_file = "test_result.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到: {output_file}")