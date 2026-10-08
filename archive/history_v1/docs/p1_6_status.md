# P1-6范围与验收说明

P1-6本次仅完成A负责的本地材料与验证。课程核心要求见project_requirement.md；另一成员重跑是团队交叉验收安排，最终模型依赖C，均不由A代填通过。

统一补充入口为根目录README_A_P1_6.md；中文材料a_cleaning_report_zh.md与英文稿a_report_section_en.md包含数据、清洗、累计量／完整性、环境／耗时、验证、局限和A的AI声明建议。英文稿仍须并入B/C章节，不是最终课程报告。

P1-6真实片段从已验收的原始整行与碰撞双侧记录提取，含1,297行、114个锚点；保留原缺槽并包含所有选定锚点的实际上下文。重跑生产函数后逐列对比正式参照，并独立Decimal复算。首次本地结果为outputs/p1_6_support/local_replay_v1/verification.json；支持包在新目录从另一工作目录重跑的收据为outputs/p1_6_support/relocated_replay_verification.json。迁移测试共享A现有Java/Python环境，不代表另一机器已安装成功。

本次支持版本 `a_support_v1` 通过独立清单绑定rainfall_v1/data_version v1，原P1-5清单及代码、配置、文档和正式表保持原哈希。支持包不含正式大表或训练模型；GitHub未发布。A本地状态以configs/processing_status.json和outputs/p1_6_support/validation.json为准。

验收包括：正式底座完整性、真实片段来源、所有清洗／标签列匹配、数量账目、86组碰撞及2处元数据标记、全部114锚点独立复算、全量报告数值／运行起止时间对账、英文稿语言、ZIP哈希与解压后实际运行。

后续待办仍为：B/C实际环境读取与独立复现；B/C真实分析与训练结果的版本记录；C最终模型及重新加载预测验证；合并全组报告、姓名／学号、AI声明；最终GitHub和Canvas发布。此次不运行模型、不操作远程仓库。
