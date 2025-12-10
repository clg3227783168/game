import json

# 读取数据
with open('code/data/final_dataset.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 分割数据
true_list = []
false_list = []

for item in data:
    if item.get('golden_sql') == True:
        true_list.append(item)
    else:
        false_list.append(item)

# 输出结果
with open('code/data/true.json', 'w', encoding='utf-8') as f:
    json.dump(true_list, f, ensure_ascii=False, indent=4)

with open('code/data/false.json', 'w', encoding='utf-8') as f:
    json.dump(false_list, f, ensure_ascii=False, indent=4)

print(f"分割完成：true.json ({len(true_list)} 条), false.json ({len(false_list)} 条)")