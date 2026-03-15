from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from hfqt.config import AppConfig


GROUPS: dict[str, dict[str, str]] = {
    "runtime": {
        "title": "基础运行",
        "description": "决定程序启动位置、数据库位置、API 监听地址和默认执行通道。",
    },
    "logging": {
        "title": "日志与审计",
        "description": "控制日志目录、滚动策略和交易流水文件名。",
    },
    "owner": {
        "title": "控制权与保护",
        "description": "控制 owner token、交易锁和本地运行控制权。",
    },
    "auth": {
        "title": "授权体系",
        "description": "控制 license、token 缓存、离线宽限和授权服务器参数。",
    },
    "trading": {
        "title": "交易与风控",
        "description": "控制观察池、自动交易、限额、置信度阈值和扫描预算。",
    },
    "data": {
        "title": "数据抓取与增强",
        "description": "控制网络情报、Alpha Vantage、价格窗口和历史相似事件匹配。",
    },
    "models": {
        "title": "模型与 Agent",
        "description": "配置主分析模型、intel agent、review agent 和降级策略。",
    },
    "translation": {
        "title": "翻译",
        "description": "控制中文翻译开关、翻译 provider 顺序和超时。",
    },
    "futu": {
        "title": "Futu 模拟盘",
        "description": "配置 OpenD 主机、端口和默认市场。",
    },
    "usmart": {
        "title": "uSmart Open API",
        "description": "配置 uSmart 的域名、渠道号、密钥、登录和交易参数。",
    },
}


ITEM_META: dict[str, dict[str, Any]] = {
    "HFQT_DEFAULT_BROKER": {
        "label": "默认执行通道",
        "description": "控制页面默认使用哪个 broker。",
        "how_to": "可选 local_paper、futu_sim、usmart。未拿到真实券商权限时建议先用 local_paper 或 futu_sim。",
        "options": ["local_paper", "futu_sim", "usmart"],
        "input_type": "select",
    },
    "HFQT_ENV_FILE": {
        "label": "Env 文件路径",
        "description": "控制系统启动时优先加载哪份环境变量文件。",
        "how_to": "默认填 .env；如果你想区分客户环境、测试环境或授权环境，可以改成自定义路径。",
    },
    "HFQT_ALLOWED_SYMBOLS": {
        "label": "允许交易标的",
        "description": "限制系统能扫描和下单的股票池。",
        "how_to": "用英文逗号分隔，例如 US.IBIT,US.MSTR,US.COIN。",
    },
    "HFQT_SCAN_CACHE_TTL_SECONDS": {
        "label": "扫描缓存秒数",
        "description": "同一观察池在缓存时间内不会重新抓全网。",
        "how_to": "更小更实时，但更慢更耗接口；建议 30-180 秒。",
    },
    "HFQT_SCAN_INCREMENTAL_TTL_SECONDS": {
        "label": "增量刷新秒数",
        "description": "在完整扫描缓存期间，允许用增量结果快速刷新。",
        "how_to": "建议 10-45 秒；用于5秒闭环与低延迟刷新。",
    },
    "HFQT_SETTLEMENT_WINDOWS": {
        "label": "结算窗口配置",
        "description": "跨时区/夜盘结算窗口日历（JSON 列表）。",
        "how_to": "按 JSON 列表配置 name/tz/days/start/end。",
    },
    "HFQT_DECISION_LOG_FILENAME": {
        "label": "决策链日志文件名",
        "description": "全链路决策记录 JSONL 文件名。",
        "how_to": "默认 decision_chain.jsonl；可按需修改。",
    },
    "HFQT_AUTO_TRADE_ENABLED": {
        "label": "默认自动交易",
        "description": "决定实时看板是否默认允许自动执行 Top 候选。",
        "how_to": "仅建议在风控和券商通道确认后开启。",
        "input_type": "checkbox",
    },
    "HFQT_AUTO_TRADE_COOLDOWN_MINUTES": {
        "label": "自动交易冷却分钟",
        "description": "同一标的一段时间内不重复下单。",
        "how_to": "建议 5-30 分钟，避免同一条情报反复触发。",
    },
    "HFQT_AUTO_TRADE_MAX_ORDERS_PER_CYCLE": {
        "label": "单轮最多自动下单数",
        "description": "每次自动刷新最多执行多少笔单。",
        "how_to": "首版建议 1，后续再逐步放开。",
    },
    "HFQT_MAX_NOTIONAL_PER_ORDER": {
        "label": "单笔最大名义金额",
        "description": "风控会用 数量 x 价格 检查是否超额。",
        "how_to": "例如 100000 表示单笔不得超过 10 万。",
    },
    "HFQT_MAX_ORDERS_PER_DAY": {
        "label": "每日最大订单数",
        "description": "防止日内交易过多。",
        "how_to": "建议先保守设置，例如 10-30。",
    },
    "HFQT_MIN_CONFIDENCE": {
        "label": "最低置信度",
        "description": "信号低于这个阈值就不会进入执行。",
        "how_to": "常用 0.55-0.70；越高越保守。",
    },
    "HFQT_NETWORK_INTEL_MAX_NEWS_AGE_HOURS": {
        "label": "新闻最大时效小时",
        "description": "超过这个时效的新闻默认不进入候选。",
        "how_to": "盘中想更实时可设 1-4；太小会导致候选减少。",
    },
    "HFQT_NETWORK_INTEL_PRIMARY_AGE_HOURS": {
        "label": "消息优先时效小时",
        "description": "系统优先使用这个窗口内的最新消息，超过后会明显降权。",
        "how_to": "建议设为 2；表示优先使用最近 2 小时内的数据。",
    },
    "HFQT_NETWORK_INTEL_MAX_SOCIAL_AGE_HOURS": {
        "label": "社媒最大时效小时",
        "description": "超过这个时效的 X 舆情默认不进入候选。",
        "how_to": "建议比新闻更短，例如 1-2 小时。",
    },
    "HFQT_NETWORK_INTEL_IMPORTANT_EVENT_AGE_HOURS": {
        "label": "重大事件参考时效小时",
        "description": "只有战争、制裁、破产等重大事件才允许超出常规窗口，作为参考项保留。",
        "how_to": "建议 24-72；仅用于重大事件参考，普通旧消息不会因为这个值继续影响结果。",
    },
    "HFQT_NETWORK_INTEL_IGNORE_QUERY": {
        "label": "忽略抓取关键词",
        "description": "开启后不会依赖查询关键词，而使用更宽的抓取范围。",
        "how_to": "建议保持 true 以避免关键词过窄导致漏报。",
        "input_type": "checkbox",
    },
    "HFQT_BTC_PROXY_SYMBOLS": {
        "label": "BTC 相关代理股票池",
        "description": "定义哪些标的会额外接入 BTC 专属情报层，例如 ETF flow、Whale Alert 和大 V 监控。",
        "how_to": "用英文逗号分隔，例如 US.IBIT,US.MSTR,US.COIN,US.MARA,US.RIOT,US.BITO。",
    },
    "HFQT_X_MONITOR_ENABLED": {
        "label": "指定 X 账号监控开关",
        "description": "控制是否专门盯指定的大 V 和官方账号，而不只是全网关键词搜索。",
        "how_to": "建议开启；关闭后仍会保留普通 xreach 舆情搜索。",
        "input_type": "checkbox",
    },
    "HFQT_X_MONITOR_ACCOUNTS": {
        "label": "指定 X 账号名单",
        "description": "配置需要重点盯的账号列表，例如 Michael Saylor、BlackRock、ETF 分析师和 Whale Alert。",
        "how_to": "只写 handle，不要带 @，用英文逗号分隔，例如 saylor,BlackRock,EricBalchunas。",
    },
    "HFQT_X_MONITOR_POSTS_LIMIT": {
        "label": "指定 X 账号抓取上限",
        "description": "每轮最多从指定账号搜索多少条相关帖子。",
        "how_to": "数值越大覆盖越全，但会更耗时间；建议 5-12。",
    },
    "HFQT_XREACH_ENABLED": {
        "label": "X 全网关键词搜索开关",
        "description": "控制是否启用 xreach 的关键词搜索（默认关闭以避免依赖关键词）。",
        "how_to": "建议保持关闭，仅在临时需要扩展覆盖时开启。",
        "input_type": "checkbox",
    },
    "HFQT_WHALE_ALERT_ENABLED": {
        "label": "Whale Alert 监控开关",
        "description": "控制是否抓取 Whale Alert 这类链上大额转账异动。",
        "how_to": "建议对 BTC 相关交易开启。",
        "input_type": "checkbox",
    },
    "HFQT_WHALE_ALERT_HANDLE": {
        "label": "Whale Alert 账号",
        "description": "配置默认抓取的链上大额异动账号。",
        "how_to": "默认填 whale_alert 即可；如后续切换其他链上提醒账号，可在这里改。",
    },
    "HFQT_WHALE_ALERT_MIN_BTC": {
        "label": "Whale Alert 最小 BTC 数量",
        "description": "只保留不低于这个数量的 BTC 大额转账，避免把小额噪音也塞进分析。",
        "how_to": "例如 500 表示只看 500 BTC 以上异动。",
    },
    "HFQT_WHALE_ALERT_MIN_USD": {
        "label": "Whale Alert 最小 USD 金额",
        "description": "当 BTC 数量不足时仍可用 USD 规模过滤大额异动。",
        "how_to": "例如 5000000 表示只看 500 万美元以上异动。",
    },
    "HFQT_BTC_ETF_FLOW_ENABLED": {
        "label": "BTC ETF Flow 开关",
        "description": "控制是否抓取 11 只比特币 ETF 的结构化净流入/流出数据。",
        "how_to": "建议开启；没有这层时，系统只能从新闻里间接感知 ETF flow。",
        "input_type": "checkbox",
    },
    "HFQT_BTC_ETF_FLOW_JINA_URL": {
        "label": "BTC ETF Flow 数据入口",
        "description": "当前默认通过可读代理抓取 Farside 的 BTC ETF flow 页面。",
        "how_to": "默认值通常就够用；如后续你有更稳定的数据源，可以替换成自己的地址。",
    },
    "HFQT_BTC_ETF_FLOW_LOOKBACK_DAYS": {
        "label": "BTC ETF Flow 回看天数",
        "description": "抓取 ETF flow 时用于构造近期趋势摘要的回看窗口。",
        "how_to": "建议 3-10；窗口越大越平滑，越小越敏感。",
    },
    "HFQT_BTC_ETF_FUNDS": {
        "label": "BTC ETF 基金列表",
        "description": "用于过滤并汇总 BTC ETF flow 的基金代码列表。",
        "how_to": "默认覆盖 11 只 ETF；可用英文逗号分隔自定义。",
    },
    "HFQT_MACRO_EVENT_ENABLED": {
        "label": "宏观事件流开关",
        "description": "控制是否加入 Fed 讲话、CPI、非农等宏观事件摘要。",
        "how_to": "建议对 BTC 和高 Beta 股票开启。",
        "input_type": "checkbox",
    },
    "HFQT_MACRO_FED_RSS_URL": {
        "label": "Fed 讲话 RSS",
        "description": "Fed 官员讲话的官方 RSS 数据源。",
        "how_to": "默认使用 Federal Reserve 官方 RSS，一般无需修改。",
    },
    "HFQT_MACRO_CPI_RELEASE_URL": {
        "label": "CPI 发布摘要入口",
        "description": "用于抓取最新 CPI 发布摘要的页面地址。",
        "how_to": "默认使用 BLS 页面可读代理；如你有更稳的宏观接口，可以替换。",
    },
    "HFQT_MACRO_NFP_RELEASE_URL": {
        "label": "非农发布摘要入口",
        "description": "用于抓取最新非农就业数据摘要的页面地址。",
        "how_to": "默认使用 BLS 页面可读代理；如你有更稳的宏观接口，可以替换。",
    },
    "HFQT_MACRO_EVENT_LIMIT": {
        "label": "宏观事件条数上限",
        "description": "每轮最多带入多少条宏观事件到分析上下文。",
        "how_to": "建议 3-6；太多会拖慢推理并引入噪音。",
    },
    "HFQT_MACRO_RSS_SOURCES": {
        "label": "宏观 RSS 源",
        "description": "宏观官方新闻/公告的 RSS 源列表。",
        "how_to": "用英文逗号分隔；默认写死官方源。",
    },
    "HFQT_ETF_NEWS_SOURCES": {
        "label": "ETF 新闻源",
        "description": "ETF 行业新闻 RSS 源列表。",
        "how_to": "用英文逗号分隔；默认写死官方/行业源。",
    },
    "HFQT_ANNOUNCEMENT_SOURCES": {
        "label": "公告源",
        "description": "公告/监管披露 RSS 源列表。",
        "how_to": "用英文逗号分隔；默认写死官方源。",
    },
    "HFQT_MACRO_CALENDAR_SOURCES": {
        "label": "宏观日历来源",
        "description": "用于轮询宏观日历的来源列表。",
        "how_to": "默认已配置常用来源，可用英文逗号分隔自定义。",
    },
    "HFQT_MACRO_CALENDAR_KEYWORDS": {
        "label": "宏观日历关键词",
        "description": "用于宏观日历扫描的关键事件名称。",
        "how_to": "默认已覆盖 CPI/NFP/FOMC 等，可用英文逗号分隔自定义。",
    },
    "HFQT_ALPHA_VANTAGE_API_KEY": {
        "label": "Alpha Vantage API Key",
        "description": "用于补充结构化新闻情绪和市场状态。",
        "how_to": "填你自己的免费或付费 key；界面不会回显旧值。",
        "input_type": "password",
    },
    "HFQT_FINANCIAL_DATASETS_ENABLED": {
        "label": "Financial Datasets 开关",
        "description": "控制是否加入 Financial Datasets 的结构化财务、估值和基本面快照。",
        "how_to": "拿到 API key 后建议开启；这层主要用来补强基本面，不只依赖新闻。",
        "input_type": "checkbox",
    },
    "HFQT_FINANCIAL_DATASETS_API_KEY": {
        "label": "Financial Datasets API Key",
        "description": "用于读取 Financial Datasets 数据源的密钥。",
        "how_to": "填入你的 vendor key；界面不会回显旧值。",
        "input_type": "password",
    },
    "HFQT_FINANCIAL_DATASETS_BASE_URL": {
        "label": "Financial Datasets API 地址",
        "description": "Financial Datasets 的结构化 REST 数据入口。",
        "how_to": "默认值可直接使用；只有在供应商更换域名或你有私有网关时才修改。",
    },
    "HFQT_FINANCIAL_DATASETS_MCP_URL": {
        "label": "Financial Datasets MCP 地址",
        "description": "Financial Datasets 官方 MCP 服务入口，用于后续 MCP 联调。",
        "how_to": "默认填官方 MCP URL 即可；当前应用主要用同源结构化 API 做稳定接入。",
    },
    "HFQT_FINANCIAL_DATASETS_STATEMENT_LIMIT": {
        "label": "Financial Datasets 财报回看条数",
        "description": "控制每次用于财务摘要的历史回看条数。",
        "how_to": "建议 3-6；越大越能看到长期趋势，但返回也更重。",
    },
    "HFQT_LLM_BASE_URL": {
        "label": "主模型 Base URL",
        "description": "OpenAI-compatible 模型服务地址。",
        "how_to": "例如本地 vLLM: http://127.0.0.1:8001/v1。",
    },
    "HFQT_LLM_PROVIDER": {
        "label": "主模型 Provider",
        "description": "主分析模型使用的 provider 标识。",
        "how_to": "常见可填 deepseek、openai、glm；如果留空，系统会按 base URL 和 key 自动推断。",
    },
    "HFQT_LLM_API_KEY": {
        "label": "主模型 API Key",
        "description": "主分析模型调用所需密钥。",
        "how_to": "本地兼容层可填 dummy；云端需填真实 key。",
        "input_type": "password",
    },
    "HFQT_LLM_MODE": {
        "label": "模型档位",
        "description": "控制主力、保底或自动降级。",
        "how_to": "primary=主力 32B，fallback=保底 14B，auto=自动降级。",
        "input_type": "select",
        "options": ["primary", "fallback", "auto"],
    },
    "HFQT_LLM_MODEL_PRIMARY": {
        "label": "主力模型",
        "description": "正常情况下优先使用的模型名。",
        "how_to": "例如 deepseek-chat 或你本地 32B 模型名。",
    },
    "HFQT_LLM_MODEL_FALLBACK": {
        "label": "保底模型",
        "description": "主模型超时或输入太重时自动切换到的模型。",
        "how_to": "例如你本地 14B 模型名。",
    },
    "HFQT_INTEL_AGENT_MODEL": {
        "label": "抓取 Agent 模型",
        "description": "用于对多源情报做去噪和摘要的模型。",
        "how_to": "可留空，留空时默认回落到主分析模型。",
    },
    "HFQT_INTEL_AGENT_ENABLED": {
        "label": "抓取 Agent 开关",
        "description": "控制抓取后的情报是否先交给本地模型做去噪和提炼。",
        "how_to": "本地模型稳定后建议开启；如果只想保留原始抓取结果，可暂时关闭。",
        "input_type": "checkbox",
    },
    "HFQT_INTEL_AGENT_PROVIDER": {
        "label": "抓取 Agent Provider",
        "description": "抓取 Agent 使用的模型服务提供方标识。",
        "how_to": "常见可填 deepseek、openai、glm；留空时默认跟主分析模型一致。",
    },
    "HFQT_INTEL_AGENT_BASE_URL": {
        "label": "抓取 Agent Base URL",
        "description": "抓取 Agent 使用的本地或远程 OpenAI-compatible 地址。",
        "how_to": "如果抓取 Agent 和主分析模型共用同一个本地端点，可以留空自动继承。",
    },
    "HFQT_INTEL_AGENT_API_KEY": {
        "label": "抓取 Agent API Key",
        "description": "抓取 Agent 调用所需的 key。",
        "how_to": "本地兼容端点通常可填 dummy；如果留空会自动继承主模型 key。",
        "input_type": "password",
    },
    "HFQT_REVIEW_AGENT_MODEL": {
        "label": "复核 Agent 模型",
        "description": "用于二次复核的模型。",
        "how_to": "可留空，留空时默认回落到主分析模型。",
    },
    "HFQT_REVIEW_AGENT_ENABLED": {
        "label": "复核 Agent 开关",
        "description": "控制是否在分析完成后再走一层模型复核。",
        "how_to": "建议在本地模型稳定后开启；如果只想跑单模型决策，可暂时关闭。",
        "input_type": "checkbox",
    },
    "HFQT_REVIEW_AGENT_SECONDARY_ENABLED": {
        "label": "复核 Agent 双审开关",
        "description": "开启后会启用第二复核模型进行交叉验证。",
        "how_to": "建议与主复核模型使用不同模型或端点。",
        "input_type": "checkbox",
    },
    "HFQT_REVIEW_AGENT_SECONDARY_PROVIDER": {
        "label": "复核 Agent 双审 Provider",
        "description": "第二复核模型的 provider 标识。",
        "how_to": "可填 deepseek、openai、glm；留空时默认跟主复核一致。",
    },
    "HFQT_REVIEW_AGENT_SECONDARY_BASE_URL": {
        "label": "复核 Agent 双审 Base URL",
        "description": "第二复核模型的 OpenAI-compatible 地址。",
        "how_to": "与主复核模型不同的端点地址。",
    },
    "HFQT_REVIEW_AGENT_SECONDARY_API_KEY": {
        "label": "复核 Agent 双审 API Key",
        "description": "第二复核模型的调用密钥。",
        "how_to": "留空时默认继承主模型 key；敏感值不回显。",
        "input_type": "password",
    },
    "HFQT_REVIEW_AGENT_SECONDARY_MODEL": {
        "label": "复核 Agent 双审模型",
        "description": "第二复核模型的模型名。",
        "how_to": "建议使用不同模型以增强交叉验证。",
    },
    "HFQT_REVIEW_CONFLICT_ACTION": {
        "label": "双审冲突处理",
        "description": "当双审结论冲突时的处理方式。",
        "how_to": "支持 hold 或 reject；默认 hold。",
    },
    "HFQT_REVIEW_AGENT_PROVIDER": {
        "label": "复核 Agent Provider",
        "description": "复核 Agent 使用的模型服务提供方标识。",
        "how_to": "可填 deepseek、openai、glm；留空时默认跟主分析模型一致。",
    },
    "HFQT_REVIEW_AGENT_BASE_URL": {
        "label": "复核 Agent Base URL",
        "description": "复核 Agent 使用的 OpenAI-compatible 地址。",
        "how_to": "和主模型共用时可留空，系统会自动继承主模型地址。",
    },
    "HFQT_REVIEW_AGENT_API_KEY": {
        "label": "复核 Agent API Key",
        "description": "复核 Agent 调用所需的 key。",
        "how_to": "留空时默认继承主模型 key；敏感值界面不回显。",
        "input_type": "password",
    },
    "HFQT_RISK_AGENT_ENABLED": {
        "label": "风控 Agent 开关",
        "description": "控制是否先让本地模型做风险审查，再叠加硬规则风控。",
        "how_to": "建议和本地分析模型一起开启；如果只想保留纯规则风控，可以关闭。",
        "input_type": "checkbox",
    },
    "HFQT_RISK_AGENT_PROVIDER": {
        "label": "风控 Agent Provider",
        "description": "风控 Agent 使用的模型服务提供方标识。",
        "how_to": "可填 deepseek、openai、glm；留空时默认跟主分析模型一致。",
    },
    "HFQT_RISK_AGENT_BASE_URL": {
        "label": "风控 Agent Base URL",
        "description": "风控 Agent 使用的 OpenAI-compatible 地址。",
        "how_to": "和主模型共用时可留空，系统会自动继承主模型地址。",
    },
    "HFQT_RISK_AGENT_API_KEY": {
        "label": "风控 Agent API Key",
        "description": "风控 Agent 调用所需的 key。",
        "how_to": "留空时默认继承主模型 key；敏感值界面不回显。",
        "input_type": "password",
    },
    "HFQT_RISK_AGENT_MODEL": {
        "label": "风控 Agent 模型",
        "description": "用于本地模型风险审查的模型。",
        "how_to": "可留空，留空时默认继承主分析模型。",
    },
    "HFQT_TRANSLATE_TO_ZH": {
        "label": "默认翻译为中文",
        "description": "控制抓取结果和分析理由是否默认翻译。",
        "how_to": "内部调试建议关闭，给客户演示时可开启。",
        "input_type": "checkbox",
    },
    "HFQT_FUTU_HOST": {
        "label": "Futu OpenD 主机",
        "description": "本地或远程 OpenD 网关地址。",
        "how_to": "本机一般填 127.0.0.1。",
    },
    "HFQT_FUTU_PORT": {
        "label": "Futu OpenD 端口",
        "description": "OpenD 默认监听端口。",
        "how_to": "默认 11111，除非你自己改过。",
    },
    "HFQT_FUTU_SECURITY_FIRM": {
        "label": "Futu 券商标识",
        "description": "少数 Futu / moomoo 接口在连接时需要额外传入券商标识。",
        "how_to": "如果你的 OpenD 或 SDK 文档要求填写，再按实际值填入；否则可以留空。",
    },
    "USMART_TRADE_HOST": {
        "label": "uSmart 交易域名",
        "description": "uSmart 交易接口基础域名。",
        "how_to": "按官方文档或券商最新通知填写。",
    },
    "USMART_QUOTE_HOST": {
        "label": "uSmart 行情域名",
        "description": "uSmart 行情 REST 域名。",
        "how_to": "按官方文档填写。",
    },
    "USMART_WS_HOST": {
        "label": "uSmart 推送域名",
        "description": "uSmart 行情推送 WebSocket 地址。",
        "how_to": "按官方文档填写 wss 地址。",
    },
    "USMART_X_CHANNEL": {
        "label": "uSmart X-Channel",
        "description": "uSmart 给渠道分配的请求头标识。",
        "how_to": "由券商或合作者提供。",
    },
    "USMART_PUBLIC_KEY": {
        "label": "uSmart 公钥",
        "description": "uSmart API 签名使用的公钥。",
        "how_to": "通过安全渠道填写，界面不会回显旧值。",
        "input_type": "password",
    },
    "USMART_PRIVATE_KEY": {
        "label": "uSmart 私钥",
        "description": "uSmart API 签名使用的私钥。",
        "how_to": "通过安全渠道填写，界面不会回显旧值。",
        "input_type": "password",
    },
    "USMART_LOGIN_PASSWORD": {
        "label": "uSmart 登录密码",
        "description": "账户登录密码。",
        "how_to": "只在你自己填写 .env 时输入，界面不回显。",
        "input_type": "password",
    },
    "USMART_TRADE_PASSWORD": {
        "label": "uSmart 交易密码",
        "description": "下单或交易登录所需密码。",
        "how_to": "只在你自己填写 .env 时输入，界面不回显。",
        "input_type": "password",
    },
}


SECRET_TOKENS = ("PASSWORD", "TOKEN", "API_KEY", "PRIVATE_KEY", "SESSION_TOKEN")
NON_SECRET_KEYS = {
    "HFQT_REQUIRE_OWNER_TOKEN_FOR_WRITE",
    "HFQT_AUTH_TOKEN_CACHE_PATH",
    "HFQT_AUTH_TOKEN_REFRESH_MINUTES",
}


def _ensure_logging_meta() -> None:
    """Placeholder for backward compatibility.

    Older versions used this hook to dynamically inject logging-related
    metadata into ITEM_META. 当前版本的日志相关配置已经在 ITEM_META 中写死，
    因此这里保持空实现即可，避免 NameError。
    """
    return None


def build_config_catalog(config: AppConfig) -> dict[str, Any]:
    _ensure_logging_meta()
    env = os.environ
    template_defaults = _load_template_defaults()
    groups: dict[str, dict[str, Any]] = {
        group_id: {
            "id": group_id,
            "title": meta["title"],
            "description": meta["description"],
            "items": [],
        }
        for group_id, meta in GROUPS.items()
    }

    for key in _load_all_known_keys():
        group_id = _group_for_key(key)
        current_value = _resolve_current_value(config, env, key)
        if current_value in (None, "") and key in template_defaults:
            current_value = template_defaults[key]
        meta = ITEM_META.get(key, {})
        secret = _is_secret_key(key, meta)
        groups[group_id]["items"].append(
            {
                "key": key,
                "label": meta.get("label") or _humanize_key(key),
                "description": meta.get("description") or _default_description(group_id, key),
                "how_to": meta.get("how_to") or _default_how_to(group_id, key, current_value),
                "input_type": meta.get("input_type") or _infer_input_type(key, current_value),
                "options": meta.get("options") or [],
                "secret": secret,
                "configured": bool(str(current_value).strip()) if current_value not in (None, False) else bool(current_value),
                "value": "" if secret else _serialize_value(current_value),
            }
        )

    ordered_groups = [group for group in groups.values() if group["items"]]
    return {"groups": ordered_groups}


def _template_paths() -> list[Path]:
    return [Path.cwd() / ".env.example", Path.cwd() / ".env.usmart.example"]


def _load_template_keys() -> list[str]:
    keys: list[str] = []
    for key in _load_template_defaults():
        keys.append(key)
    return keys


def _load_template_defaults() -> dict[str, str]:
    defaults: dict[str, str] = {}
    for path in _template_paths():
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key and key not in defaults:
                defaults[key] = value.strip()
    return defaults


def _load_all_known_keys() -> list[str]:
    keys = _load_template_keys()
    seen = set(keys)
    config_text = (Path.cwd() / "src" / "hfqt" / "config.py").read_text(encoding="utf-8")
    for key in _extract_config_keys(config_text):
        if key not in seen:
            keys.append(key)
            seen.add(key)
    return keys


def _extract_config_keys(config_text: str) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for key in re.findall(r'env\.get\("([A-Z0-9_]+)"', config_text):
        if key not in seen:
            keys.append(key)
            seen.add(key)
    for key in re.findall(r'os\.environ\.get\("([A-Z0-9_]+)"', config_text):
        if key not in seen:
            keys.append(key)
            seen.add(key)
    for key in re.findall(r'_first_present\(env,\s*"([A-Z0-9_]+)"', config_text):
        if key not in seen:
            keys.append(key)
            seen.add(key)
    return keys


def _group_for_key(key: str) -> str:
    if key.startswith("USMART_"):
        return "usmart"
    if key.startswith("HFQT_FUTU_"):
        return "futu"
    if key.startswith("HFQT_AUTH_"):
        return "auth"
    if key.startswith("HFQT_OWNER_") or key in {"HFQT_REQUIRE_OWNER_TOKEN_FOR_WRITE", "HFQT_TRADING_LOCKED_DEFAULT"}:
        return "owner"
    if key.startswith("HFQT_LLM_") or key.startswith("HFQT_INTEL_AGENT_") or key.startswith("HFQT_REVIEW_AGENT_") or key.startswith("HFQT_RISK_AGENT_"):
        return "models"
    if key.startswith("HFQT_TRANSLATION_") or key == "HFQT_TRANSLATE_TO_ZH":
        return "translation"
    if key.startswith("HFQT_LOG_") or key.endswith("_LOG_FILENAME"):
        return "logging"
    if key.startswith("HFQT_ALPHA_VANTAGE_") or key.startswith("HFQT_YAHOO_"):
        return "data"
    if key.startswith("HFQT_X_MONITOR_") or key.startswith("HFQT_XREACH_") or key.startswith("HFQT_WHALE_ALERT_") or key.startswith("HFQT_BTC_ETF_FLOW_") or key.startswith("HFQT_MACRO_"):
        return "data"
    if key in {
        "HFQT_NETWORK_INTEL_LIMIT",
        "HFQT_NETWORK_INTEL_PRIMARY_AGE_HOURS",
        "HFQT_NETWORK_INTEL_MAX_NEWS_AGE_HOURS",
        "HFQT_NETWORK_INTEL_MAX_SOCIAL_AGE_HOURS",
        "HFQT_NETWORK_INTEL_IMPORTANT_EVENT_AGE_HOURS",
        "HFQT_YFINANCE_NEWS_LIMIT",
        "HFQT_BTC_PROXY_SYMBOLS",
        "HFQT_HISTORY_MATCH_LIMIT",
        "HFQT_HISTORY_MATCH_LOOKBACK_DAYS",
        "HFQT_INTRADAY_FEATURE_WINDOW_MINUTES",
    }:
        return "data"
    if key in {
        "HFQT_DATABASE_PATH",
        "HFQT_API_HOST",
        "HFQT_API_PORT",
        "HFQT_DEFAULT_BROKER",
        "HFQT_DRY_RUN",
    }:
        return "runtime"
    return "trading"


def _resolve_current_value(config: AppConfig, env: os._Environ[str], key: str) -> Any:
    if key in env and env.get(key) not in (None, ""):
        return env.get(key)

    attribute_name = _attribute_name_for_key(key)
    if attribute_name and hasattr(config, attribute_name):
        return getattr(config, attribute_name)
    return ""


def _attribute_name_for_key(key: str) -> str | None:
    if key.startswith("HFQT_"):
        return key[5:].lower()
    if key.startswith("USMART_"):
        return f"usmart_{key[7:].lower()}"
    return None


def _serialize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value)
    return str(value)


def _humanize_key(key: str) -> str:
    normalized = key.replace("HFQT_", "").replace("USMART_", "uSmart ")
    return normalized.replace("_", " ").strip().title()


def _is_secret_key(key: str, meta: dict[str, Any]) -> bool:
    if key in NON_SECRET_KEYS:
        return False
    if meta.get("input_type") == "password":
        return True
    return any(token in key for token in SECRET_TOKENS)


def _infer_input_type(key: str, value: Any) -> str:
    rendered = _serialize_value(value).strip().lower()
    if rendered in {"true", "false"}:
        return "checkbox"
    if any(token in key for token in SECRET_TOKENS):
        return "password"
    if any(token in key for token in ("PORT", "LIMIT", "COUNT", "MINUTES", "HOURS", "BYTES", "DAYS", "SECONDS", "BUDGET_MS", "PCT", "QUANTITY")):
        return "number"
    return "text"


def _default_description(group_id: str, key: str) -> str:
    label = _humanize_key(key)
    if group_id == "runtime":
        return f"控制 {label} 的基础运行参数。"
    if group_id == "logging":
        return f"控制 {label} 的日志与审计行为。"
    if group_id == "owner":
        return f"控制 {label} 的本地控制权和交易保护策略。"
    if group_id == "auth":
        return f"控制 {label} 的授权校验、离线宽限或 license 行为。"
    if group_id == "trading":
        return f"控制 {label} 的交易、风控、观察池或自动执行行为。"
    if group_id == "data":
        return f"控制 {label} 的行情抓取、新闻情报或数据增强逻辑。"
    if group_id == "models":
        return f"控制 {label} 的模型、Agent 或降级策略。"
    if group_id == "translation":
        return f"控制 {label} 的翻译开关、顺序或超时设置。"
    if group_id == "futu":
        return f"控制 {label} 的 Futu OpenD 模拟盘接入。"
    if group_id == "usmart":
        return f"控制 {label} 的 uSmart Open API 接入参数。"
    return f"配置 {label}。"


def _default_how_to(group_id: str, key: str, current_value: Any) -> str:
    if _is_secret_key(key, {}):
        return "这是敏感参数，只建议由用户自己写入本地 .env；界面不会回显旧值。"
    if group_id == "models":
        return "通常写入 .env 后重启服务生效；如果走本地 OpenAI-compatible 端点，请同时确认 base URL、model 和 key。"
    if group_id == "data":
        return "按数据源能力填写，修改后写入 .env 并重启；实时性越高通常越耗接口和时间预算。"
    if group_id == "trading":
        return "修改后写入 .env 并重启；建议先从保守值开始，逐步放开自动交易和阈值。"
    if group_id in {"futu", "usmart"}:
        return "按券商文档或实际联调信息填写，写入 .env 后重启；敏感字段请通过安全方式自行填入。"
    if group_id == "auth":
        return "建议先在测试环境核对 license 或 token 逻辑，确认后再写入正式 .env。"
    if isinstance(current_value, bool):
        return "填写 true 或 false，写入 .env 后重启服务生效。"
    return "修改后写入本地 .env，并重启服务生效。"
