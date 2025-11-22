# SQL 复杂度分析报告

> 基于 `code/data/true.json` 数据集的 SQL 语句复杂度分析

---

## 一、数据集概览

- **总记录数**: 15 条
- **复杂度分布**:
  - 简单: 3 条 (20%)
  - 中等: 8 条 (53.3%)
  - 复杂: 4 条 (26.7%)

---

## 二、核心指标对比表

| 特征指标 | 简单 | 中等 | 复杂 |
|---------|------|------|------|
| **平均表数量** | 5.33 | 6.38 | 19.50 |
| **平均聚合函数数量** | 2.33 | 6.62 | 14.00 |
| **平均子查询数量** | 3.00 | 4.12 | 16.25 |
| **平均窗口函数数量** | 0.33 | 0.00 | 0.75 |
| **平均CASE WHEN数量** | 0.00 | 2.38 | 15.75 |
| **GROUP BY使用率** | 66.7% | 87.5% | 100% |
| **ORDER BY使用率** | 33.3% | 12.5% | 50% |
| **UNION使用率** | 33.3% | 25.0% | 75% |
| **CTE (WITH)使用率** | 0.0% | 25.0% | 75% |
| **平均SQL字符数** | 968 | 1,719 | 8,441 |
| **平均SQL行数** | 32.0 | 46.8 | 187.2 |

---

## 三、各复杂度详细分析

### 🟢 简单复杂度 (3条)

#### 核心特点
- SQL相对简短，平均 **968 字符**，**32 行**
- 基本不使用CASE WHEN逻辑
- 很少使用窗口函数（仅33.3%使用）
- 不使用CTE（WITH子句）
- 子查询数量适中（平均3个）

#### 技术特征
- **JOIN类型**: 主要使用INNER JOIN (75%)，少量LEFT JOIN (25%)
- **聚合函数**: 主要是COUNT函数
- **DISTINCT使用率**: 100%（说明需要去重）
- **WHERE条件复杂率**: 100%（虽然是简单SQL，但WHERE条件较多）

#### 典型示例 (sql_45)
```sql
select count(distinct a.vplayerid) as player_num
from (
    select distinct vplayerid
    from dws_jordass_matchlog_stat_di
    where dtstatdate between '20240101' and '20240107'
    and imode = 1287652322611036928
) a
left join (
    select distinct vplayerid
    from dws_jordass_matchlog_stat_di
    where dtstatdate between '20240108' and '20240114'
    and imode = 1287652322611036928
) b on a.vplayerid = b.vplayerid
join (
    select distinct vplayerid
    from dws_jordass_matchlog_stat_di
    where dtstatdate between '20240115' and '20240121'
    and imode = 1287652322611036928
) c on a.vplayerid = c.vplayerid
where b.vplayerid is null;
```

#### 简单SQL特征总结
- ✅ 查询逻辑清晰，单一目标
- ✅ 使用2-3个子查询进行数据筛选
- ✅ 主要通过JOIN和WHERE进行数据关联和筛选
- ✅ 使用基础聚合函数（COUNT）
- ✅ 不涉及复杂的业务逻辑转换

---

### 🟡 中等复杂度 (8条)

#### 核心特点
- SQL长度中等，平均 **1,719 字符**，**47 行**
- 开始使用CASE WHEN（平均2.38个）
- 聚合函数使用明显增多（平均6.62个）
- 25%使用CTE优化代码结构
- 25%使用UNION进行结果合并

#### 技术特征
- **JOIN类型**: 主要使用LEFT JOIN (90%)，说明需要保留左表所有数据
- **聚合函数分布**: COUNT(8次)、SUM(2次)、MIN(1次)
- **GROUP BY使用率**: 87.5%（大部分需要分组统计）
- **WHERE条件复杂率**: 50%（条件复杂度适中）

#### 典型示例 (sql_33)
```sql
select
    count(distinct a.vplayerid) as total_players,
    count(distinct case when b.vplayerid is not null then a.vplayerid end) as existing_players,
    count(distinct case when b.vplayerid is null then a.vplayerid end) as new_players
from (
    select vplayerid
    from dws_jordass_matchlog_stat_di
    where dtstatdate between '20241214' and '20241220'
    and imode = 1344338933661592832
    and platid = 255
    group by vplayerid
) a
left join (
    select vplayerid
    from dws_jordass_playermatchrecord_stat_df
    where dtstatdate = '20241213'
    and imode = 1344338933661592832
    and platid = 255
    group by vplayerid
) b
on a.vplayerid = b.vplayerid;
```

#### 中等SQL特征总结
- ✅ 涉及新老用户区分、留存分析等中等复杂业务逻辑
- ✅ 使用CASE WHEN进行条件判断和分类
- ✅ 需要多次聚合计算
- ✅ 部分使用UNION合并多个时间段/玩法的数据
- ✅ 开始使用CTE提高可读性

---

### 🔴 复杂复杂度 (4条)

#### 核心特点
- SQL非常长，平均 **8,441 字符**，**187 行**
- 大量使用CASE WHEN（平均15.75个）进行业务逻辑转换
- 涉及大量表关联（平均19.5个表）
- 子查询数量多（平均16.25个）
- 75%使用CTE进行代码组织
- 75%使用UNION合并多个数据源

#### 技术特征
- **JOIN类型**: LEFT JOIN和INNER JOIN混合使用
- **聚合函数分布**: COUNT、SUM、MAX、MIN多种函数组合使用
- **窗口函数**: ROW_NUMBER、LEAD等高级分析函数
- **GROUP BY使用率**: 100%（必须进行分组统计）
- **业务逻辑**: 涉及多玩法、多时间段、多用户类型的综合分析

#### 典型示例 (sql_30 - 玩法主玩情况统计)
```sql
with main_user as (
    select substr(dtstatdate, 1, 6) mons,
        case
            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'
            when modename = '传统模式' and mapname = '群屿' then '传统群屿'
            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'
            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'
            when submodename = '广域战场模式' then '广域战场'
            when submodename = '极能形态模式' then '极能形态'
            when modename = '组队竞技' then '组竞'
            when modename = '乐园' then '乐园'
            when modename = '领地' then '领地'
            when modename = '广阔天地' then '广阔天地'
            else '其他模式'
        end imodename,
        vplayerid,
        sum(roundtime) / 60 roundtime,
        sum(roundcnt) roundcnt
   from dws_jordass_mode_roundrecord_di
   where dtstatdate between '20190508' and '20250330'
   group by vplayerid, substr(dtstatdate, 1, 6), ...
   -- 后续还有大量的嵌套查询和JOIN操作
)
-- ... 更多复杂的查询逻辑
```

#### 复杂SQL特征总结
- ✅ 跨越长时间范围的数据分析（如2019-2025）
- ✅ 涉及多种玩法、多种用户类型的交叉分析
- ✅ 大量CASE WHEN进行业务规则映射（如玩法分类、用户类型分类）
- ✅ 使用CTE分层构建复杂查询逻辑
- ✅ UNION合并8-10个不同玩法的数据
- ✅ 需要ROW_NUMBER等窗口函数进行排序和筛选
- ✅ 涉及4-5层嵌套子查询

---

## 四、复杂度分级规则总结

基于以上分析，可以总结出清晰的复杂度分级规则：

### 🟢 简单级别标准

| 指标 | 标准 |
|------|------|
| **SQL长度** | < 1,500 字符，< 50 行 |
| **表数量** | ≤ 6 个 |
| **子查询** | ≤ 3 个 |
| **CASE WHEN** | 不使用或 ≤ 1 个 |
| **UNION** | 不使用或仅1次 |
| **CTE** | 不使用 |
| **窗口函数** | 不使用或仅1个简单窗口函数 |
| **业务逻辑** | 单一、清晰的查询目标 |

### 🟡 中等级别标准

| 指标 | 标准 |
|------|------|
| **SQL长度** | 1,500-5,000 字符，50-130 行 |
| **表数量** | 6-10 个 |
| **子查询** | 4-8 个 |
| **CASE WHEN** | 2-5 个 |
| **聚合函数** | 5-10 个 |
| **UNION** | 1-2 次 |
| **CTE** | 可选使用 |
| **业务逻辑** | 涉及用户分类、留存分析等中等复杂场景 |

### 🔴 复杂级别标准

| 指标 | 标准 |
|------|------|
| **SQL长度** | > 5,000 字符，> 130 行 |
| **表数量** | > 10 个 |
| **子查询** | > 10 个 |
| **CASE WHEN** | > 10 个 |
| **聚合函数** | > 10 个 |
| **UNION** | ≥ 3 次 |
| **CTE** | 必须使用（提高可读性） |
| **窗口函数** | 使用复杂窗口函数 |
| **业务逻辑** | 多维度交叉分析、长时间跨度、多玩法综合统计 |

---

## 五、关键发现

### 1. 复杂度与SQL长度高度相关
复杂SQL的平均长度是简单SQL的 **8.7倍**（8,441 vs 968字符），SQL长度是判断复杂度的直观指标。

### 2. CASE WHEN是复杂度关键指标
- 简单SQL：平均 0 个
- 中等SQL：平均 2.38 个
- 复杂SQL：平均 15.75 个

CASE WHEN的数量直接反映了业务逻辑的复杂程度。

### 3. CTE使用体现代码质量
- 简单SQL：0% 使用
- 中等SQL：25% 使用
- 复杂SQL：75% 使用

复杂SQL中大量使用CTE来提高代码可读性和可维护性。

### 4. UNION使用反映数据整合需求
- 简单SQL：33.3% 使用
- 中等SQL：25.0% 使用
- 复杂SQL：75% 使用

复杂SQL往往需要合并多个数据源（不同玩法、不同时间段）。

### 5. 所有复杂度都100%使用DISTINCT
说明在游戏数据分析场景中，数据去重是基本需求，无论查询复杂度如何。

---

## 六、补充分析维度

### 查询类型分布
- **单表查询**: 主要出现在简单SQL中
- **多表JOIN**: 所有复杂度都涉及，但复杂SQL的表数量显著更多

### JOIN类型偏好
- **简单SQL**: 倾向于INNER JOIN（75%），强调精确匹配
- **中等SQL**: 倾向于LEFT JOIN（90%），需要保留主表数据
- **复杂SQL**: 混合使用，根据业务需求灵活选择

### 聚合函数复杂度
- **简单SQL**: 主要使用COUNT
- **中等SQL**: COUNT + SUM/MIN等基础聚合
- **复杂SQL**: 多种聚合函数组合，且与CASE WHEN结合使用

### 窗口函数使用
- **简单SQL**: 基本不使用（仅33.3%）
- **中等SQL**: 完全不使用（0%）
- **复杂SQL**: 75% 使用 ROW_NUMBER、LEAD 等高级分析函数

---

## 七、总结

本次分析基于 `code/data/true.json` 文件中的 15 条 SQL 记录，通过 11 项关键指标的量化统计，明确了简单、中等、复杂三个复杂度等级的判定标准：

- **简单SQL**: 单一目标、清晰逻辑、基础聚合
- **中等SQL**: 用户分类、留存分析、条件判断
- **复杂SQL**: 多维交叉、长时跨度、业务映射

复杂度评估的核心指标包括：
1. SQL长度（字符数和行数）
2. CASE WHEN 数量（最关键）
3. 表数量和子查询数量
4. CTE和UNION的使用
5. 窗口函数的复杂度

这些分级标准可以用于指导 Text-to-SQL 模型的训练和评估，帮助模型更好地理解不同复杂度的SQL语句特征。
