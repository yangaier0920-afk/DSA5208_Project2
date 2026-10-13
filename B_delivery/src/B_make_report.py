"""Generate a copyable Chinese B report from measured outputs; no model results invented."""
import argparse,calendar,json
from pathlib import Path
from b_common import PACKAGE,rows,csv_write,dump

def table(headers,records):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
        ['| '+' | '.join(str(v) for v in r)+' |' for r in records])+'\n'

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--task1',type=Path,default=PACKAGE/'results/task1_v2')
    ap.add_argument('--features',type=Path,default=PACKAGE/'results/features_v2')
    ap.add_argument('--output',type=Path,default=PACKAGE/'report_zh');a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    f=json.loads((a.task1/'findings.json').read_text());v=json.loads((a.features/'validation.json').read_text())
    tm=json.loads((a.task1/'result_metadata.json').read_text());fm=json.loads((a.features/'result_metadata.json').read_text())
    hardware=json.loads((PACKAGE/'results/hardware_inventory.json').read_text())
    retention=[]
    for r in rows(a.features/'yearly_validation.csv'):
        n=int(r['rows']);k=int(r['history_complete']);retention.append({'year':int(r['year']),'anchors':n,'history_complete':k,'history_incomplete':n-k,'retention_rate':k/n,'excluded_rate':1-k/n})
    total=sum(r['anchors'] for r in retention);kept=sum(r['history_complete'] for r in retention)
    csv_write(a.features/'history_retention_by_year.csv',retention)
    sr=rows(a.task1/'strict_same_month_summary.csv');sensitivity=[]
    for m in range(1,13):
        x=[r for r in sr if int(r['month'])==m and float(r['coverage_cutoff'])==.9 and r['mean_station_month_observed_mm']]
        y=[r for r in sr if int(r['month'])==m and float(r['coverage_cutoff'])==.95 and r['mean_station_month_observed_mm']]
        if not x or not y:continue
        def extrema(rr):
            return int(min(rr,key=lambda r:float(r['mean_station_month_observed_mm']))['year']),int(max(rr,key=lambda r:float(r['mean_station_month_observed_mm']))['year'])
        lo90,hi90=extrema(x);lo95,hi95=extrema(y)
        yy={r['year']:float(r['mean_station_month_observed_mm']) for r in y}
        differences=[abs(yy[r['year']]-float(r['mean_station_month_observed_mm'])) for r in x]
        sensitivity.append(dict(month=m,stations_90=int(x[0]['n_stations']),stations_95=int(y[0]['n_stations']),lowest_year_90=lo90,lowest_year_95=lo95,highest_year_90=hi90,highest_year_95=hi95,extrema_consistent=lo90==lo95 and hi90==hi95,max_abs_mean_difference_mm=max(differences)))
    csv_write(a.task1/'coverage_sensitivity_summary.csv',sensitivity)
    freq=rows(a.task1/'monthly_frequency_intensity_summary.csv');freqs={c:[r for r in freq if float(r['coverage_cutoff'])==c] for c in [.9,.95]}
    peaks={c:(max(rr,key=lambda r:float(r['wet_fraction'])),max(rr,key=lambda r:float(r['mean_station_wet_p90_5m_mm']))) for c,rr in freqs.items()}
    freqdiff=max(abs(float(next(r for r in freqs[.95] if r['month']==x['month'])['wet_fraction'])-float(x['wet_fraction'])) for x in freqs[.9])*100
    parts=[]
    def add(s):parts.append(s.strip()+'\n')
    def figure(name,caption):
        import os
        path=Path(__import__('os').path.relpath(a.task1/'figures'/name,a.output)).as_posix()
        add(f'![{caption}]({path})\n\n{caption}')
    add('''# 新加坡降雨规律分析与历史特征构建：B部分中文报告

版本：2026-10-13 Task 1逻辑重排与图表样式统一；数值特征版本仍为 B_v2。本文可直接复制、修改并合并进组报告。

**报告范围**：B负责Task 1及供C使用的历史特征，本文不重复C模型训练及预测评价。合并后的仓库已包含C成果及全组英文提交材料，见C_delivery与submission；本稿是更新后的B中文章节。''')
    add(f'''## 1. 数据处理与分析口径

使用2017—2024年正式发布的 {total:,} 条清洗观测。Spark完成原始规模的统计与特征处理，Matplotlib只读取小型汇总表作图。B读取A冻结发布并校验文件哈希和schema；月度观测数量与Decimal雨量累计已对照A的区间日历覆盖表，核对6,528个站点—月份条目。缺测始终表示未知，真实零雨量保留为零，不任意填补或截断有效极端值。完整清洗方法由A在最终报告说明。

Task 1采用Asia/Singapore时区的区间起点归属年月和小时。基本有雨指标为五分钟雨量>0；月累计为实际观测累计，不按覆盖率补齐。站点在同一格内等权，跨年描述图再对可用年份等权。''')
    add((PACKAGE/'docs/TIME_CONTRACT_zh.md').read_text().replace('# 预测时间与雨量区间：A/B/C 交接口径','## 2. 预测时间、观测可用性与标签区间').replace('\n## ','\n### ' ).replace('### 2. 预测时间','## 2. 预测时间'))
    add('''## 3. Task 1：数据覆盖与比较范围

覆盖门槛决定哪些比较成立。主分析采用月覆盖率≥90%，另以≥95%做敏感性分析。严格八年同月共同站点仅支持5、9、10、12月；其他月份空交集不表示没有降雨。''')
    figure('01_coverage_and_panel.png','图1：各年各月合格站点数，以及八年同月共同站点数量。')
    add('下面先讨论年内的月份、降雨频率与小时规律，再讨论跨年变化，最后比较同一面板内的站点差异。')
    add('## 4. Task 1：年内降雨规律')
    add('### 4.1 月度观测雨量概览')
    figure('02_descriptive_monthly_rainfall.png','图2：各年各月合格站点的平均观测累计雨量，NA为无合格站点。')
    add('描述性格子中，2022年10月最高，为419.1 mm（66站）；2021年2月最低，为32.0 mm（68站）。这同时受天气、观测覆盖及站点组成影响，不是固定站网跨年排名；各月天数也不同。')
    add('### 4.2 降雨频率与条件强度')
    figure('07_frequency_and_intensity.png','图3：有雨槽比例与条件于有雨的五分钟雨量P50/P90/P99。')
    add(f'''90%门槛下，描述性有雨槽比例最高为{int(peaks[.9][0]['month'])}月（{100*float(peaks[.9][0]['wet_fraction']):.2f}%）；平均非零P90最高为{int(peaks[.9][1]['month'])}月（{float(peaks[.9][1]['mean_station_wet_p90_5m_mm']):.2f} mm/五分钟）。发生频率和条件强度是不同指标，月累计还受月份时长影响。

分位数使用Spark percentile_approx（accuracy=1000），先计算站点—月份的非零分位数，再对合格站点及可用年份等权平均；**不是全国汇集观测的分位数**。全干站点—月份频率为0，但条件强度分位数未知，不补成0。来源表保留强度指标各自的支持数量。''')
    add('### 4.3 一天中的降雨发生规律')
    figure('04_hourly_rain_occurrence.png','图4：新加坡时间下月×小时有雨比例，以及全天剖面。')
    add('小时比较要求一个站点—月份全部24小时各自均≥90%覆盖，以相同站点构成比较一天各小时。先对站点等权，再对82个受支持年—月等权。峰值在16时，为5.78%；最低在23时，为1.11%。这些是已记录五分钟槽的有雨比例，不是未来30分钟超阈值概率，也不能证明物理因果机制。')
    add('### 4.4 天气背景与结果解释\n\n本项目的描述结果中，11月有雨槽比例较高，小时剖面在16时达到峰值。MSS官方气候介绍指出，新加坡通常11—1月雨量较多，下午降雨也较频繁；东北季风涌动及午后对流是相关天气背景。因此，这些观察与已知气候背景相容，有助于理解年内和日内差异，但本项目没有将个别降雨过程分类，也未控制风场、海风或其他气象变量，不能据此证明产生这些结果的物理机制。[MSS：Climate of Singapore](https://www.weather.gov.sg/climate-climate-of-singapore/)（查阅：2026-10-13）\n\n官方长期气候资料与本项目的时间段、站点、覆盖筛选和指标定义不同；本项目的五分钟有雨槽比例也不等同于官方“雨日数”。不能将不同来源的数值直接作同口径比较，也不能把某一月份的短期异常解释成长期气候趋势。')
    add('## 5. Task 1：跨年降雨变化')
    add('### 5.1 年度概览与可比性限制')
    annual=[r for r in rows(a.task1/'annual_descriptive_summary.csv') if float(r['coverage_cutoff'])==.9]
    add('年度覆盖率分母是完整日历年五分钟槽数，正确处理闰年；先汇总单站年度实际雨量，再对该年合格站点等权。另列每个月都≥90%的更严格要求，以暴露全年覆盖率可能掩盖的集中缺测。')
    add(table(['年份','全年覆盖≥90%站点数','平均观测年雨量/mm','每月均≥90%站点数','强门槛平均观测年雨量/mm'],[[r['year'],r['n_stations'],f"{float(r['mean_observed_annual_mm']):.1f}" if r['mean_observed_annual_mm'] else 'NA',r['stations_passing_every_month'],f"{float(r['mean_observed_annual_mm_each_month']):.1f}" if r['mean_observed_annual_mm_each_month'] else 'NA'] for r in annual]))
    figure('06_annual_descriptive_summary.png','图5：全年90%/95%覆盖门槛与每月均90%门槛下的年度描述及站点数。')
    add('2018没有站点达到全年90%覆盖，保留NA；八年固定年度面板为空。各年站点构成不同，年度观测累计并非补齐缺测后的完整年估计，也不能据此得出全国年度趋势。更严格逐月门槛还会使更多年份不受支持。')
    add('由于年度合格站点组成变化、八年固定年度面板为空，下面转向固定同月份站点的比较，以提高跨年可比性。')
    add('### 5.2 严格八年同月比较与覆盖门槛敏感性')
    figure('03_strict_interannual_comparison.png','图6：严格同月固定站点面板的2017—2024年比较，90%与95%覆盖门槛。')
    add(table(['月份','90%站点数','95%站点数','最低年90%/95%','最高年90%/95%','极值年一致','各年两门槛均值差的最大绝对值/mm'],[[r['month'],r['stations_90'],r['stations_95'],f"{r['lowest_year_90']}/{r['lowest_year_95']}",f"{r['highest_year_90']}/{r['highest_year_95']}",'是' if r['extrema_consistent'] else '否',f"{r['max_abs_mean_difference_mm']:.2f}"] for r in sensitivity]))
    add(r"""表中差值定义为：

$$
D_m=\max_{y\in\{2017,\ldots,2024\}}\left|\bar R_{y,m}^{(90\%)}-\bar R_{y,m}^{(95\%)}\right|.
$$

其中，$\bar R_{y,m}^{(c)}$ 是覆盖门槛 $c$ 对应八年共同站点面板在年份 $y$、月份 $m$ 的平均观测累计雨量（mm）。先对**同一年、同一月份的两种门槛**求差，再取八年中最大绝对值；不是最高年份与最低年份的雨量差。十月该敏感性差值约5.32 mm，而90%面板最高年与最低年的差约286.81 mm，两者不能混用。对应CSV字段保留为`max_abs_mean_difference_mm`，以兼容已有读取代码。

英文表头建议使用：`Maximum absolute difference between 90% and 95% panel means across years (mm)`。""")
    add(f'''90%与95%门槛支持的月份相同，收紧门槛减少站点数。上表直接核对最高/最低年份，而非笼统声称结果完全相同。平均雨量的变化同时反映被排除站点的组成差异，不是同一批站点上的独立实验。

十月90%固定21站的2022年观测均值399.8 mm，2023年113.0 mm。不同月份的最高年份不一致，不能概括某一年处处最湿，也不能把仅支持的四个月相加当全年。

频率图90%/95%门槛下的最高月份分别为{int(peaks[.9][0]['month'])}/{int(peaks[.95][0]['month'])}月，非零P90最高月份分别为{int(peaks[.9][1]['month'])}/{int(peaks[.95][1]['month'])}月；12个月有雨比例均值的最大绝对差为{freqdiff:.4f}个百分点。此敏感性反映描述口径稳定程度，不等于统计显著性检验。''')
    add('### 5.3 补充：相邻年份同月份共同站点比较')
    adjacent=rows(a.task1/'adjacent_year_same_month_summary.csv')
    valid=[r for r in adjacent if int(r['n_common_stations'])>0]
    add('保留前面的严格八年面板作为主分析，另预先固定七对相邻年份（2017—2018至2023—2024），逐月选取两年该月覆盖率均≥90%的共同站点编号。每个格子内，两年使用完全相同的站点并等权，计算后一年减前一年的平均观测累计雨量。全部84个格子均列出；没有共同站点时保留NA，不挑选有明显差异的月份。')
    add(r"""令 $I_{y,m}$ 为年份 $y$ 与 $y+1$ 在月份 $m$ 均达到90%覆盖的共同站点集合，则：

$$
\Delta_{y,m}=\frac{1}{|I_{y,m}|}\sum_{s\in I_{y,m}}\left(R_{s,y+1,m}-R_{s,y,m}\right).
$$

$R$ 为站点该月实际观测累计雨量（mm），不是缺测补齐量。正值表示共同站点在后一年该月更湿，负值表示更干；$n=|I_{y,m}|$，不是置信度或独立天气事件数量。""")
    figure('09_adjacent_year_comparison.png','图7：相邻年份同月份共同站点的平均观测雨量差（后一年−前一年，mm）；每格标注共同站点数n，雨量差显示为整数、精确值见CSV；灰色NA表示无共同站点。')
    pair_summary=[]
    for year in range(2017,2024):
        rr=[r for r in valid if int(r['year_from'])==year];nn=[int(r['n_common_stations']) for r in rr]
        pair_summary.append([f'{year}—{year+1}',len(rr),f'{min(nn)}—{max(nn)}' if nn else 'NA'])
    add(table(['相邻年份','支持月份数/12','各支持月份共同站点数范围'],pair_summary))
    upper=max(valid,key=lambda r:float(r['mean_paired_change_mm']))
    lower=min(valid,key=lambda r:float(r['mean_paired_change_mm']))
    extra=[r for r in valid if int(r['month']) not in [5,9,10,12]]
    add(f"84个局部比较中，{len(valid)}个有共同站点支持，其中{len(extra)}个来自严格八年面板不支持的其他月份。作为全部格子的描述性极值，最大增加为{upper['year_from']}→{upper['year_to']}年{upper['month']}月，平均观测雨量从{float(upper['mean_from_observed_mm']):.2f}增至{float(upper['mean_to_observed_mm']):.2f} mm，差{float(upper['mean_paired_change_mm']):+.2f} mm（{upper['n_common_stations']}站）；最大减少为{lower['year_from']}→{lower['year_to']}年{lower['month']}月，从{float(lower['mean_from_observed_mm']):.2f}变为{float(lower['mean_to_observed_mm']):.2f} mm，差{float(lower['mean_paired_change_mm']):+.2f} mm（{lower['n_common_stations']}站）。这些是遍历全部格子后的描述性最大/最小值，未作显著性或多重比较检验。")
    add('这补充了局部可比信息，但估计对象不同于固定八年面板：共同站点会随年份对和月份改变，不能把局部差值串联成全国长期趋势，也不能拼接成全年总雨量。月累计仍受残余缺测与站点坐标/测量条件影响；涉及闰年的二月还存在月份天数差异，未按天标准化。因此该图只描述局部共同站点的观测月总量变化，不用于物理归因。无需重跑原始Spark聚合；图7与表格仅读取此前已核验的站点—月份汇总。')
    add('## 6. Task 1：同一面板内的空间差异')
    add('先看站点位置，再比较雨量数值。十月拥有最大的严格90%八年同月共同站点面板，共21站，因此用于本节；同数时选较早月份。两张图使用完全相同的站点编号及雨量均值。')
    figure('08_station_map.png','图8：十月共同面板的21个站点位置，颜色表示2017—2024年平均十月观测累计雨量。')
    add('图注：位置采用A元数据中最后观测时刻最晚的坐标；S113星号表示两组记录坐标约相距39米，变化原因无法确认。固定编号不保证测量条件始终一致。详细核验保留在station_coordinate_audit.csv和spatial_panel_metadata_history.csv；地图不做空间插值。')
    figure('05_station_comparison.png','图9：与图8相同21站的平均十月观测累计雨量，按数值排序。')
    add('S89最高约270.3 mm，S107最低约125.2 mm。地图提供位置背景，柱状图便于比较大小；这些是十月共同面板的描述性差异，不能当作全年站点排名或全国空间雨量估计，也未控制地理或仪器条件的影响。')
    add('### 本节分析的共同限制')
    add('区间起点日历下有64个整网无观测日期；A原始event-date审计为63，额外日期2018-02-17源于日历定义不同。缺测日不是干日。月累计没有按覆盖率补齐，固定站点编号面板也不是经过气候均一化的站网。官方资料提示可能缺测且未经过气候资料质量控制，因此以上季节、跨年与空间差异均保持描述性定位。[data.gov.sg：Historical Rainfall across Singapore](https://data.gov.sg/collections/2279/view)（查阅：2026-10-13）')
    add('## 7. Task 2：B提供的26个历史特征')
    definitions=(PACKAGE/'docs/FEATURE_DEFINITIONS_zh.md').read_text();add(definitions[definitions.index('令 R(t)'):])
    add('单锚点预测入口features_at仅接收该站点t及以前60分钟观测，要求当前锚点和12个精确历史槽完整；已与离线26列逐一核对，不提供未来行仍能复现，并拒绝缺槽或不在五分钟网格的时刻。station编码、标准化或插补必须由C在各轮训练集拟合；阈值也是C的模型输入。')
    add('## 8. 完整历史筛选的逐年影响')
    add(table(['年份','观测锚点数','完整60分钟历史','不完整历史','历史保留率'],[[r['year'],f"{r['anchors']:,}",f"{r['history_complete']:,}",f"{r['history_incomplete']:,}",f"{100*r['retention_rate']:.3f}%"] for r in retention]+[['合计',f'{total:,}',f'{kept:,}',f'{total-kept:,}',f'{100*kept/total:.3f}%']]))
    add(f'''不完整历史共{total-kept:,}个锚点，占{100*(1-kept/total):.3f}%。B特征文件保留所有锚点，相应完整性不足的窗口汇总为null，没有从发布特征中删除它们，也没有补零。完整历史模型的监督样本排除这些锚点；已知未来标签仍保留作审计，不能因为历史缺失而改写其真实标签。2018的保留率明显较低，因此总体97.5%不能替代逐年说明。

A的model_anchor_eligible=historical_valid且target_valid。C监督训练/评价还需按本轮role、边界、训练站点支持等规则筛选；history_complete_60m与historical_valid的逐年完整数量已经匹配。未来完整和历史完整的排除会重叠，不能简单相加损失比例，也不能把本表保留率当最终训练样本率。

这些保留率属于全部观测锚点的描述，不能证明筛选后样本代表整个天气分布；最终评价仅适用于受支持且历史完整的范围。推理缺槽时当前接口拒绝预测，未来可另行开发并独立验证缺测鲁棒模型。''')
    add('## 9. 与标签及C实际输入的一对一关联核验')
    join_file=PACKAGE/'results/join_audit/validation.json'
    if join_file.exists():
        audit=json.loads(join_file.read_text());t=audit['totals']
        add(table(['核验项','A正式标签表与B特征表实测'],[['两侧行数',f"{t['left_rows']:,} / {t['right_rows']:,}"],['两侧唯一主键数',f"{t['left_unique_keys']:,} / {t['right_unique_keys']:,}"],['两侧重复超额行',f"{t['left_duplicate_excess_rows']} / {t['right_duplicate_excess_rows']}"],['两侧空主键行',f"{t['left_null_key_rows']} / {t['right_null_key_rows']}"],['两侧未匹配行',f"{t['left_unmatched_rows']} / {t['right_unmatched_rows']}"],['两侧匹配率',f"{100*t['left_match_rate']:.2f}% / {100*t['right_match_rate']:.2f}%"],['inner关联后行数',f"{t['inner_join_rows']:,}"],['left关联后行数',f"{t['left_join_rows']:,}"],['关联增加样本数',t['row_inflation']]]))
    else:add('基础表关联核验尚未运行，不能填入100%匹配结论。')
    add('''主键为station_id + prediction_epoch_s，标签按预测锚点关联，不能仅按站点或日期。全量基础表核验按年执行，并先核验主键epoch年份与分区年份一致，因此跨分区重复不会被漏掉。

**合并后证据状态：上表是A/B基础表核验；C的实际输入关联另有保存记录。** C_delivery/results/C_v1_20261012的fold1/2/3与final目录均包含actual_input_join_audit.json，状态为PASS。final记录有10,410,105个左侧唯一锚点，未匹配左侧行及关联膨胀均为0，左侧匹配率100%。这是对已有JSON的核对，本次没有重新训练或重跑该全量关联。实际输入仍应在阈值展开前核验，失败时停止，不得先dropDuplicates掩盖问题。训练子集只要求每个左侧锚点匹配，B全量右侧额外未使用行可以存在。阈值展开后同一锚点重复是预期行为，检查应改用station_id+prediction_epoch_s+threshold_mm，并核对每锚点阈值数及展开前后行数。''')
    add('## 10. 计算环境与耗时定义')
    add(table(['项目','配置/实测'],[['运行环境',f"本地{hardware['machine_name']}，{hardware['chip_type']}"],['CPU',f"{fm['environment']['logical_cpu_count']}个逻辑CPU；Spark本次使用2个任务线程"],['物理内存',hardware['physical_memory']],['系统',fm['environment']['os']],['Python / Spark / Java','3.11.9 / 3.5.7 / 17'],['Spark执行','local[2]，单机两个任务线程'],['JVM heap / shuffle','4g / 128分区'],['读取分区上限','32 MiB'],['AQE','启用；禁用分区合并'],['完整输入规模',f'{total:,}条锚点（2017—2024）'],['特征Parquet大小',f"{sum(p.stat().st_size for p in (a.features/'parquet').rglob('*.parquet')):,}字节"],['全量内存缓存','未启用'],['峰值内存','没有测量，不给出数值']]))
    add(f'''硬件清单由system_profiler在2026-10-11补查，仅保留CPU/内存等公开配置，不保存设备序列号等标识；这是同一主机的后补配置记录，不是原运行时的峰值资源测量。

原Task 1聚合源运行耗时{tm['elapsed_seconds']:.1f}秒：计时从main中time.monotonic、发布完整性验证之前开始，到三类聚合写出完成、验证JSON写出之前结束。包括发布校验、Spark启动、实际读入/聚合、汇总CSV写出；不包括后续元数据哈希、绘图、报告或Spark关闭。v2重用了这些已验证聚合表，没有再次测量Task 1全流程时间。

v2全量历史特征运行耗时{fm['elapsed_seconds']:.1f}秒：从发布验证前开始，到逐年Parquet生成、回读数量与空值检查、首次独立探针及schema/验证文件写出完成后、metadata调用前结束。包括实际读取、计算、写出和当次校验，不包括metadata输出哈希、Spark关闭、绘图、报告，以及之后单独补跑的513探针和预测接口核验。所有时间是单次本机观测，不是稳定性能基准，也没有清理或控制操作系统文件缓存。

全量关联核验的独立耗时见results/join_audit/validation.json；不合并计入上述旧运行时间。若修改窗口或工程配置，需使用新输出目录重新计时，不能沿用旧数字。''')
    add(f'''## 11. 验证、局限与后续工作

B_v2共{v['rows']:,}条特征，{v['history_complete']:,}条完整历史。逐年数量与A核对，全部窗口空值语义及完整历史26列非空检查通过。固定种子20261011抽取20个至少有20条观测的站点，每站20个锚点，结合113个实际存在的固定边界锚点，共513个独立时间戳字典核验；另有六个合成湿/干、缺槽、跨年案例。关联函数另有重复左右键、缺匹配、空键和正常匹配五个案例检查。

限制包括：项目区间与回溯可用性假设、发布延迟尚未验证、站点组成变化、缺测、近似条件分位数、仅完整历史模型的适用范围，以及没有气象因果控制或显著性检验。新增持续性特征是候选，不能在没有C验证指标时宣称提升了预测性能。

C已提交上述三轮时间验证、锁定后重拟合、2024最终评价和模型重载材料，见C_delivery及submission；本次合并保留其保存结果，不重新评价或挑选模型。后续任何模型改进仍必须遵守训练期拟合与测试集使用边界。B的Task 1描述2024不表示允许用2024标签挑选特征或模型。本文没有虚构模型分数。

## 12. AI使用声明

OpenAI Codex协助B进行分析设计、Spark特征与关联核验代码、图表、说明和报告中文草稿。数量与结论来自实际运行及保存证据，自动检查不等于人工逐行审阅。组员须理解假设与特征定义、核对结果，并据实际使用合并全组AI声明。最终提交仍需全组完整源代码与运行README、训练模型、列出成员姓名学号的最终PDF；本文为B部分可编辑材料。''')
    path=a.output/'B_report_zh.md';path.write_text('\n\n'.join(parts),encoding='utf-8')
    dump(PACKAGE/'results/report_metadata/report_sources.json',{'language':'zh-CN','format':'Markdown only','feature_set_version':'B_v2',
        'source_task1':str(a.task1),'source_features':str(a.features),'source_join_audit':str(join_file),
        'C_actual_input_audit':'STORED_PASS_RECORDS_PRESENT_NOT_RERUN',
        'source_C_actual_input_audit':'C_delivery/results/C_v1_20261012/{fold1,fold2,fold3,final}/actual_input_join_audit.json','numeric_features_recomputed':False,'documentation_revision':'2026-10-13',
        'source_adjacent_years':str(a.task1/'adjacent_year_validation.json'),
        'figure_reading_order':['01_coverage_and_panel','02_descriptive_monthly_rainfall','07_frequency_and_intensity','04_hourly_rain_occurrence','06_annual_descriptive_summary','03_strict_interannual_comparison','09_adjacent_year_comparison','08_station_map','05_station_comparison']})
    print('Chinese report saved:',path)

if __name__=='__main__':main()
