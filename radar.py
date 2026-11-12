#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
商品期货消息面黑天鹅雷达 (Commodity Black-Swan Radar)
=====================================================
每 30 分钟扫描一次全球 + 国内财经快讯，匹配国内 79 个商品期货品种，
对"突发性、影响走势"的事件做分级告警，并叠加盘面异动确认。

用法：
    python3 radar.py --once      # 跑一次（供 cron / 计划任务调用）
    python3 radar.py --daemon    # 常驻进程，每 30 分钟自循环
    python3 radar.py --check     # 自检：数据源连通性 + 配置完整性

输出：
    alerts/latest.md    最新一次扫描报告（人读，供 AI 播报）
    alerts/latest.json  最新一次扫描结果（机读）
    alerts/YYYY-MM-DD.jsonl  当日历史告警流水
    logs/radar.log      运行日志
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

BASE = Path(__file__).resolve().parent
CFG_PATH = BASE / "config.json"
ALERT_DIR = BASE / "alerts"
LOG_DIR = BASE / "logs"
STATE_DIR = BASE / "state"
STATE_PATH = STATE_DIR / "state.json"
LOG_PATH = LOG_DIR / "radar.log"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
TIMEOUT = 15
LOOKBACK_MIN = int(os.environ.get("RADAR_LOOKBACK_MIN", "65"))  # 单次扫描回看窗口（可用环境变量临时放大，供停更后补扫）
FIRST_RUN_LOOKBACK_MIN = 180
CST = 8                    # UTC+8
TZ8 = timezone(timedelta(hours=CST))


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def log(msg: str) -> None:
    """写日志文件 + 同时输出到 stdout（供 systemd/journal 捕获）。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        if LOG_PATH.exists() and LOG_PATH.stat().st_size > 5 * 1024 * 1024:
            LOG_PATH.write_text("", encoding="utf-8")
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def load_config() -> dict[str, Any]:
    with CFG_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


def load_state() -> dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"seen": [], "last_run": None, "run_count": 0}


def save_state(state: dict[str, Any], cap: int) -> None:
    seen = state.get("seen", [])
    if len(seen) > cap:
        seen = seen[-cap:]
    state["seen"] = seen
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STATE_PATH)


def uid_of(prefix: str, text: str) -> str:
    """内容指纹去重：同一事件在多源出现时也只算一次。"""
    norm = re.sub(r"[\s\u3000“”\"'【】\[\]（）()，,。.、:：;；!！?？<>]", "", text)
    return prefix + hashlib.md5(norm.encode("utf-8")).hexdigest()[:16]


def get_json(url: str, headers: dict[str, str] | None = None,
             retries: int = 3, timeout: int = TIMEOUT) -> Any | None:
    """带重试的 GET。代理偶发断连时重试，失败返回 None 由调用方降级。"""
    last: Exception | str = ""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers or {"User-Agent": UA}, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            last = f"HTTP {resp.status_code}"
        except (requests.RequestException, ValueError) as exc:
            last = exc
        if attempt < retries - 1:
            time.sleep(1.0 * (attempt + 1))
    log(f"[网络] 请求失败（{retries} 次）：{url.split('?')[0]} → {last}")
    return None


def to_cst(ts: float) -> datetime:
    """时间戳 → 东八区 naive datetime（与快讯内的时间字符串口径一致）。"""
    return datetime.fromtimestamp(ts, tz=TZ8).replace(tzinfo=None)


def fmt_chg(chg: Any) -> str:
    return f"{chg:+.2f}%" if isinstance(chg, (int, float)) else "—"


# --------------------------------------------------------------------------- #
# 采集：行情
# --------------------------------------------------------------------------- #
def fetch_quotes_sina(cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """新浪期货主连行情（GBK 编码），一次请求覆盖全部品种，稳定性优于东财。"""
    sina_map = {v["sina"]: v for v in cfg["varieties"] if v.get("sina")}
    if not sina_map:
        return {}
    url = "https://hq.sinajs.cn/list=" + ",".join("nf_" + c for c in sina_map)
    headers = {"Referer": "https://finance.sina.com.cn", "User-Agent": UA}
    try:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT * 2)
        resp.encoding = "gbk"
        if resp.status_code != 200:
            log(f"[行情] 新浪源 HTTP {resp.status_code}")
            return {}
    except requests.RequestException as exc:
        log(f"[行情] 新浪源失败: {exc}")
        return {}

    sector_map = {v["code"]: v["sector"] for v in cfg["varieties"]}
    out: dict[str, dict[str, Any]] = {}
    for line in resp.text.split("\n"):
        if '="' not in line:
            continue
        code = line.split('="')[0].split("_")[-1]
        body = line.split('="', 1)[1].rstrip('";\n\r ')
        parts = body.split(",")
        if len(parts) < 15:
            continue                      # 停牌/无报价品种
        v = sina_map.get(code)
        if not v:
            continue
        try:
            last = float(parts[8])
            prev_settle = float(parts[10])
        except ValueError:
            continue
        if last <= 0 or prev_settle <= 0:
            continue
        out[v["code"]] = {
            "code": v["code"], "name": v["name"],
            "exchange": v.get("exchange", ""), "sector": sector_map.get(v["code"], ""),
            "price": last,
            "chg": round((last - prev_settle) / prev_settle * 100, 2),
            "amount": float(parts[14]) if parts[14] not in ("", None) else 0,
        }
    return out


def fetch_quotes_eastmoney(cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """东方财富期货板块接口（兜底源），抓 5 大交易所主力连续合约。"""
    out: dict[str, dict[str, Any]] = {}
    sector_map = {v["code"]: v["sector"] for v in cfg["varieties"]}
    headers = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}
    for market, ex_name in cfg["market_boards"].items():
        page = 1
        while page <= 6:
            url = ("https://push2.eastmoney.com/api/qt/clist/get"
                   f"?pn={page}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f12"
                   f"&fs=m:{market}&fields=f12,f14,f2,f3,f4,f5,f6")
            try:
                data = get_json(url, headers=headers, retries=2)
            except Exception as exc:         # noqa: BLE001
                log(f"[行情] 板块 {ex_name} 第{page}页异常: {exc}")
                break
            if not data:
                break
            data = data.get("data")
            if not data or not data.get("diff"):
                break
            for row in data["diff"]:
                name = str(row.get("f14", ""))
                # 只要「主力连续」：排除次主连、月均价、指数
                if not name.endswith("主连") or "次" in name or "月均" in name:
                    continue
                clean = name.replace("主连", "")
                code = str(row.get("f12", ""))
                price = row.get("f2")
                chg = row.get("f3")
                if not isinstance(price, (int, float)) or not isinstance(chg, (int, float)):
                    continue          # 停牌/无报价品种（如动力煤、强麦）
                out[code] = {
                    "code": code, "name": clean, "exchange": ex_name,
                    "sector": sector_map.get(code, ""),
                    "price": float(price), "chg": float(chg),
                    "amount": row.get("f6") if isinstance(row.get("f6"), (int, float)) else 0,
                }
            if page * 100 >= int(data.get("total", 0)):
                break
            page += 1
    return out


def fetch_quotes(cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """行情总入口：新浪为主、东财兜底、本地快照补空缺。"""
    out = fetch_quotes_sina(cfg)
    log(f"[行情] 新浪源 {len(out)} 个合约")
    if len(out) < 40:
        log("[行情] 新浪覆盖不足，启用东方财富兜底")
        em = fetch_quotes_eastmoney(cfg)
        for code, val in em.items():
            out.setdefault(code, val)
        log(f"[行情] 东财兜底补充后共 {len(out)} 个合约")

    # 代理抖动导致的品种缺失：用上一次快照补齐，避免报告出现大片空缺
    cache_path = STATE_DIR / "quotes_cache.json"
    if cache_path.exists():
        try:
            prev = json.loads(cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            prev = {}
        missing = [c for c in prev if c not in out]
        for code in missing:
            out[code] = {**prev[code], "stale": True}
        if missing:
            log(f"[行情] 缺失 {len(missing)} 个合约，已用上次快照补齐（标注为延迟数据）")

    # 只有采到有效数据才覆盖缓存，避免一次网络故障清空历史
    if len(out) >= 40:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(
            {c: {k: v for k, v in q.items() if k != "stale"} for c, q in out.items()},
            ensure_ascii=False), encoding="utf-8")

    log(f"[行情] 共 {len(out)} 个商品期货主力连续合约")
    return out


# --------------------------------------------------------------------------- #
# 采集：消息面
# --------------------------------------------------------------------------- #
def fetch_wscn(channel: str, limit: int = 100) -> list[dict[str, Any]]:
    """华尔街见闻 7x24 快讯（全球 / 商品两个频道）。"""
    url = ("https://api-one.wallstcn.com/apiv1/content/lives"
           f"?channel={channel}&limit={limit}")
    resp = get_json(url, {"User-Agent": UA})
    if not resp:
        return []
    items = resp.get("data", {}).get("items", []) or []
    news = []
    for it in items:
        text = (it.get("content_text") or it.get("title") or "").strip()
        if len(text) < 8:
            continue
        ts = it.get("display_time")
        if not isinstance(ts, (int, float)):
            continue
        news.append({
            "uid": uid_of("wscn", text),
            "time": to_cst(float(ts)),
            "source": "华尔街见闻" if channel == "global-channel" else "华尔街见闻·商品",
            "text": text,
            "important": bool(it.get("score", 0) and it.get("score") >= 2),
            "url": (it.get("article") or {}).get("uri", ""),
        })
    return news


def fetch_sina(pages: int = 2) -> list[dict[str, Any]]:
    """新浪财经 7x24 直播（国内财经/产业快讯）。"""
    news = []
    for page in range(1, pages + 1):
        url = ("https://zhibo.sina.com.cn/api/zhibo/feed"
               f"?page={page}&page_size=50&zhibo_id=152&tag_id=0&dire=f&dpc=1")
        try:
            feed = get_json(url, {"User-Agent": UA})
            rows = (feed or {})["result"]["data"]["feed"]["list"]
        except (KeyError, TypeError) as exc:
            log(f"[新浪] 第{page}页解析失败: {exc}")
            break
        for row in rows:
            text = re.sub(r"<[^>]+>", "", str(row.get("rich_text", ""))).strip()
            if len(text) < 8:
                continue
            try:
                t = datetime.strptime(str(row.get("create_time", "")), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            news.append({
                "uid": uid_of("sina", text),
                "time": t,
                "source": "新浪财经7x24",
                "text": text,
                "important": False,
                "url": "",
            })
    return news


def fetch_jin10() -> list[dict[str, Any]]:
    """金十数据快讯（高频、带重要度标记）。"""
    headers = {"User-Agent": UA, "x-app-id": "bVBF4FyRTn5NJF5n", "x-version": "1.0.0"}
    url = "https://flash-api.jin10.com/get_flash_list?channel=-8200&vip=1"
    try:
        payload_json = get_json(url, headers)
        rows = (payload_json or {}).get("data", []) or []
    except Exception as exc:                 # noqa: BLE001
        log(f"[金十] 失败: {exc}")
        return []
    news = []
    for row in rows:
        inner = row.get("data") or {}
        text = (inner.get("content") or inner.get("title") or "").strip()
        text = re.sub(r"<[^>]+>", "", text)
        if len(text) < 8:
            continue
        try:
            t = datetime.strptime(str(row.get("time", "")), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            t = datetime.now()
        news.append({
            "uid": uid_of("jin10", text),
            "time": t,
            "source": "金十数据",
            "text": text,
            "important": bool(row.get("important")),
            "url": inner.get("source_link") or "",
        })
    return news


def fetch_news() -> tuple[list[dict[str, Any]], list[str]]:
    """并发拉取所有消息源，单源失败不影响整体。"""
    sources = [
        ("华尔街见闻·全球", lambda: fetch_wscn("global-channel", 100)),
        ("华尔街见闻·商品", lambda: fetch_wscn("commodity-channel", 60)),
        ("新浪财经7x24", lambda: fetch_sina(2)),
        ("金十数据", fetch_jin10),
    ]
    all_news: list[dict[str, Any]] = []
    errors: list[str] = []
    for name, fn in sources:
        try:
            got = fn()
            all_news.extend(got)
            log(f"[消息] {name}: {len(got)} 条")
        except Exception as exc:            # noqa: BLE001 单源失败降级
            errors.append(f"{name}: {exc}")
            log(f"[消息] {name} 采集失败: {exc}")
    return all_news, errors


# --------------------------------------------------------------------------- #
# 判定：品种匹配 + 事件分级 + 评分
# --------------------------------------------------------------------------- #
def build_index(cfg: dict[str, Any]) -> dict[str, Any]:
    """预编译正则，避免逐条新闻重复编译。"""
    def alias_pattern(words: list[str]) -> str:
        """英文/数字缩写加词边界，避免 PF Holding 误撞「短纤 PF」。"""
        parts = []
        for w in words:
            if re.fullmatch(r"[A-Za-z0-9%./+-]+", w):
                parts.append(rf"(?<![A-Za-z0-9]){re.escape(w)}(?![A-Za-z0-9])")
            else:
                parts.append(re.escape(w))
        return "|".join(parts)

    varieties = []
    for v in cfg["varieties"]:
        aliases = [a for a in v.get("aliases", []) if len(a) >= 2]
        drivers = [d for d in v.get("drivers", []) if len(d) >= 2]
        varieties.append({
            "code": v["code"], "name": v["name"], "sector": v["sector"],
            "alias_re": re.compile(alias_pattern(aliases)) if aliases else None,
            "driver_re": re.compile(alias_pattern(drivers)) if drivers else None,
        })
    crit_re = re.compile("|".join(map(re.escape, cfg["event_words"]["critical"])))
    mjor_re = re.compile("|".join(map(re.escape, cfg["event_words"]["major"])))
    rout_re = re.compile("|".join(map(re.escape, cfg["event_words"].get("routine", []))) or r"$^")
    macro_re = re.compile("|".join(map(re.escape, cfg["event_words"]["macro_global"])))
    noise_re = re.compile("|".join(map(re.escape, cfg.get("noise_words", []))) or r"$^")
    neg_re = {code: re.compile("|".join(map(re.escape, words)))
              for code, words in cfg.get("negative_words", {}).items() if words}
    anchor_re = {code: re.compile("|".join(map(re.escape, words)))
                 for code, words in cfg.get("anchor_words", {}).items() if words}
    return {"varieties": varieties, "crit": crit_re, "major": mjor_re,
            "routine": rout_re, "macro": macro_re, "noise": noise_re, "neg": neg_re,
            "anchor": anchor_re, "weak_drivers": set(cfg.get("weak_drivers", []))}


# 期货语境词：出现时才认定谈论的是商品本身而非同名公司/产品
FUTURES_CONTEXT_RE = re.compile(r"(期货|主力|合约|盘面|现货|仓单|产区|冷库|入库|基差|持仓|交割|现货价)")


# 纯行情播报（"XX主力合约涨超5%"），不构成事件驱动
QUOTE_REPORT_RE = re.compile(
    r"(主力合约|收盘|收涨|收跌|日内涨|日内跌|涨超|跌超|现报|报每|报价|盘面|涨幅|跌幅|涨停|跌停)"
)
NOISE_RE = re.compile(r"(股价|评级|目标价|回购|增发|减持|业绩|财报|营收|净利润|公告|研报)")


def scan_news(news: list[dict[str, Any]], idx: dict[str, Any]) -> list[dict[str, Any]]:
    """把每条快讯映射到受影响品种，产出候选命中记录。

    品种归属规则：别名直接命中优先；仅当命中 ≥2 个强指向 driver 且同期存在
    重大/突发事件词时才允许 driver 单独归属，避免「美国/进口」这类通用词误报。
    """
    hits: list[dict[str, Any]] = []
    for item in news:
        text = item["text"]
        crit = idx["crit"].findall(text)
        major = idx["major"].findall(text)
        routine = idx["routine"].findall(text)
        macro = idx["macro"].findall(text)
        if not (crit or major):
            continue                       # 无事件词 = 纯资讯，不进入告警评估

        has_ctx = bool(FUTURES_CONTEXT_RE.search(text))
        matched: list[dict[str, Any]] = []
        for v in idx["varieties"]:
            # 强语境锚点：如「苹果」必须同时出现产区/冷库等农业语境，否则是苹果公司
            anchor = idx["anchor"].get(v["code"])
            if anchor and not anchor.search(text):
                continue
            # 排除同名公司/产品语境（苹果公司 vs 苹果期货）
            neg = idx["neg"].get(v["code"])
            if neg and neg.search(text) and not has_ctx:
                continue
            alias_hit = v["alias_re"].search(text) if v["alias_re"] else None
            if alias_hit:
                matched.append({"code": v["code"], "name": v["name"], "sector": v["sector"],
                                "hit_type": "alias", "word": alias_hit.group(0)})
                continue
            if v["driver_re"] is not None:
                drivers = set(v["driver_re"].findall(text))
                strong = drivers - idx["weak_drivers"]
                # 产地/地区词跨品种共享，单个命中不足以锁定品种
                if (strong or len(drivers) >= 2) and crit:
                    matched.append({"code": v["code"], "name": v["name"], "sector": v["sector"],
                                    "hit_type": "driver", "word": "、".join(sorted(drivers)[:3])})

        if not matched:
            continue
        hits.append({
            "news": item,
            "varieties": matched,
            "critical_words": crit,
            "major_words": major,
            "routine_words": routine,
            "macro_words": macro,
        })
    log(f"[匹配] {len(news)} 条快讯中 {len(hits)} 条命中品种关键词")
    return hits


CORP_NOISE_RE = re.compile(
    r"递交.{0,4}辞职|辞职报告|辞去.{0,6}职务|离任|监事|股东大会|职工代表|"
    r"公司章程|聘任|股东减持|股权变动|信息披露违规|副董事长辞职|董事长辞职")


STOCK_NOISE_RE = re.compile(
    r"研报|评级|增持|买入|目标价|业绩预告|净利润|营收|毛利率|市盈率|"
    r"机构调研|分析师|评级上调|评级下调|首次覆盖|维持.{0,4}评级")


def detect_direction(text: str, cfg: dict[str, Any]) -> tuple[str, str, list[str]]:
    """判事件对品种的方向与强度。

    以标题（【…】或开头 60 字）为准：正文常带背景描述，会把主旨带偏。
    返回 (bull/bear/neutral, 强/中/弱, 依据词)。
    """
    d = cfg.get("direction", {}) or {}
    t = (text or "").strip()
    m = re.match(r"^【(.{2,60}?)】", t)
    core = m.group(1) if m else t[:60]

    def collect(seg: str) -> tuple[list[str], list[str]]:
        return ([w for w in d.get("bull", []) if w in seg],
                [w for w in d.get("bear", []) if w in seg])

    cb, cs = collect(core)
    if cb or cs:
        bull, bear = cb, cs
    elif m:
        bull, bear = [], []          # 有标题但标题无方向词，正文背景不作数
    else:
        bull, bear = collect(t)
    wb = sum(2 if w in d.get("strong", []) else 1 for w in bull)
    ws = sum(2 if w in d.get("strong", []) else 1 for w in bear)
    direction = "bull" if wb > ws else ("bear" if ws > wb else "neutral")
    if any(w in t for w in d.get("strong", [])):
        impact = "强"
    elif any(w in t for w in d.get("mid", [])) or cumulative_hit_count(core, d):
        impact = "中"
    else:
        impact = "弱"
    basis = (bull if direction == "bull" else bear if direction == "bear" else [])[:3]
    return direction, impact, basis


def cumulative_hit_count(seg: str, d: dict[str, Any]) -> bool:
    """同一段里利多或利空词出现两个以上，也按中等强度算。"""
    n = sum(1 for w in d.get("bull", []) if w in seg) + \
        sum(1 for w in d.get("bear", []) if w in seg)
    return n >= 2


def score_hit(hit: dict[str, Any], quotes: dict[str, dict[str, Any]],
              cfg: dict[str, Any], idx: dict[str, Any]) -> dict[str, Any]:
    """给命中记录打分并定级。"""
    th = cfg["thresholds"]
    news = hit["news"]
    text = news["text"]

    # 研报/评级/公司业绩类与商品供需无关，不做告警
    if STOCK_NOISE_RE.search(text):
        return {}
    # 公司人事与治理公告，除非同时带供给冲击词
    if CORP_NOISE_RE.search(text) and not any(
            w in text for w in ("停产", "减产", "限产", "罢工", "事故", "爆炸", "火灾", "出口禁令")):
        return {}

    # 行情播报类不做事件告警（但仍进入异动归因池）
    if QUOTE_REPORT_RE.search(text) and not any(
            w in text for w in ("停产", "停产整顿", "出口禁令", "制裁", "罢工", "地震", "洪水", "飓风")):
        return {}

    if hit["critical_words"]:
        base = 6.0 + (1.0 if len(set(hit["critical_words"])) > 1 else 0.0)
    elif hit["major_words"]:
        base = 3.0
    else:
        return {}

    noise_penalty = 4.0 if idx["noise"].search(text) else 0.0

    direction, impact, dir_basis = detect_direction(text, cfg)
    best: dict[str, Any] = {}
    for v in hit["varieties"]:
        q = quotes.get(v["code"], {})
        chg = abs(q.get("chg", 0) or 0)
        s = base
        s += 3.0 if v["hit_type"] == "alias" else 1.5
        s += 0.5 if hit.get("routine_words") else 0.0
        if news.get("important"):
            s += 2.0
        if chg >= 6:
            s += 4.0
        elif chg >= 4:
            s += 3.0
        elif chg >= 2.5:
            s += 1.5
        s -= noise_penalty

        cand = {
            "variety": v["name"], "code": v["code"], "sector": v["sector"],
            "hit_type": v["hit_type"], "hit_word": v["word"],
            "critical": list(dict.fromkeys(hit["critical_words"]))[:4],
            "major": list(dict.fromkeys(hit["major_words"]))[:4],
            "macro": list(dict.fromkeys(hit["macro_words"]))[:4],
            "price": q.get("price"), "chg": q.get("chg"),
            "score": round(s, 1),
            "direction": direction, "impact": impact, "dir_basis": dir_basis,
            "time": news["time"].strftime("%H:%M"),
            "source": news["source"],
            "text": text[:300], "url": news.get("url", ""),
        }
        # 定级：L1 = 突发强事件 + 品种别名直接命中 + 总分达标
        if cand["critical"] and v["hit_type"] == "alias" and s >= th["L1_score"]:
            cand["level"] = "L1"
        elif cand["critical"] and s >= th["L2_score"]:
            cand["level"] = "L2"
        elif v["hit_type"] == "alias" and s >= th["L2_score"]:
            cand["level"] = "L2"
        elif s >= 3.0:
            cand["level"] = "L3"
        else:
            cand["level"] = "L4"

        if not best or cand["score"] > best["score"]:
            best = cand
    return best


MACRO_RULES: list[tuple[str, str, list[str], str]] = [
    # (类别, 触发词, 受影响板块, 必须出现的商品传导对象)
    ("地缘冲突", "战争|军事冲突|交火|空袭|袭击|导弹|制裁|禁运|封锁|海峡封锁|运河堵塞|霍尔木兹|曼德海峡|封港|政变",
     ["能化", "航运", "贵金属"],
     "原油|油价|能源|天然气|运价|运费|航运|集装箱|金属|黄金|白银|供应链"),
    ("货币政策", "美联储|FOMC|鲍威尔",
     ["贵金属", "有色"],
     "美元|黄金|白银|铜|铝|商品|大宗|贵金属|美债|利率"),
    ("极端天气", "地震|海啸|飓风|台风|洪水|干旱|霜冻|冰雹|蝗灾|极寒|热浪|野火",
     ["农产品", "能化"],
     "大豆|玉米|小麦|棉花|白糖|棕榈|橡胶|咖啡|可可|产区|作物|农作物|农产品|矿山|油田"),
    ("贸易政策", "关税|出口禁令|禁止出口|配额限制",
     ["有色", "黑色", "农产品"],
     "铜|铝|镍|锌|钢材|铁矿|煤炭|大豆|玉米|菜籽|棉花|棕榈|原油|金属|矿"),
    ("能源供给", "OPEC|战略储备|钻井|页岩油",
     ["能化"],
     "原油|油价|产量|减产|增产|库存|出口"),
]


def macro_scan(idx: dict[str, Any], news: list[dict[str, Any]],
               quotes: dict[str, dict[str, Any]],
               attributed: set[str]) -> list[dict[str, Any]]:
    """纯宏观/地缘事件外溢：只取未归因到具体品种、且类别明确的新闻。"""
    out = []
    sector_names = {q["sector"] for q in quotes.values()}
    for item in news:
        if item["uid"] in attributed:
            continue
        text = item["text"]
        if idx["noise"].search(text):
            continue
        for label, pattern, sectors, scope in MACRO_RULES:
            if not re.search(pattern, text):
                continue
            if not re.search(scope, text):     # 与商品无传导关系的不报
                continue
            hit_sectors = [s for s in sectors if s in sector_names]
            out.append({
                "time": item["time"].strftime("%H:%M"),
                "source": item["source"],
                "category": label,
                "text": text[:200],
                "sectors": hit_sectors,
                "level": "L2",
            })
            break
    return out[:6]


def dedupe_events(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同一品种、同一量级、同一突发事件的重复报道合并，只保留评分最高的一条。"""
    kept: list[dict[str, Any]] = []
    for a in sorted(alerts, key=lambda x: -x["score"]):
        for k in kept:
            if (k["code"] == a["code"] and k["level"] == a["level"]
                    and set(k["critical"]) & set(a["critical"])):
                k.setdefault("merged", 0)
                k["merged"] += 1
                break
        else:
            kept.append(a)
    return kept


def attribute_movers(movers: list[dict[str, Any]], news: list[dict[str, Any]],
                     idx: dict[str, Any]) -> list[dict[str, Any]]:
    """盘面异动归因：回捞本窗口内所有提到该品种的快讯，给出线索（含未被判定为告警的）。"""
    vmap = {v["code"]: v for v in idx["varieties"]}
    out = []
    for m in movers:
        v = vmap.get(m["code"])
        related = []
        if v and v["alias_re"]:
            neg = idx["neg"].get(v["code"])
            anchor = idx["anchor"].get(v["code"])
            for item in sorted(news, key=lambda x: x["time"], reverse=True):
                if not v["alias_re"].search(item["text"]):
                    continue
                if anchor and not anchor.search(item["text"]):
                    continue
                # 同名公司/产品语境（果链新闻）不算苹果期货的驱动
                if neg and neg.search(item["text"]) and not FUTURES_CONTEXT_RE.search(item["text"]):
                    continue
                related.append({"time": item["time"].strftime("%H:%M"),
                                "source": item["source"], "text": item["text"][:180]})
                if len(related) >= 3:
                    break
        out.append({**m, "related": related})
    return out


def price_movers(quotes: dict[str, dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """盘面异动：无明确消息也可能已经反映黑天鹅，单独列出。"""
    limit = cfg["thresholds"]["price_move_alert"]
    return sorted(
        [q for q in quotes.values() if abs(q["chg"]) >= limit],
        key=lambda x: abs(x["chg"]), reverse=True,
    )


# --------------------------------------------------------------------------- #
# 输出
# --------------------------------------------------------------------------- #
LEVEL_META = {
    "L1": ("🔴 P0 突发", "供应中断/制裁/灾害级事件，可能剧烈影响走势"),
    "L2": ("🟠 P1 重要", "重要变量变化，需评估对持仓的影响"),
    "L3": ("🟡 P2 观察", "一般性产业消息，仅记录"),
}


def render_report(payload: dict[str, Any]) -> str:
    macro = payload["macro"]
    alerts = payload["alerts"]
    movers, stats = payload["movers"], payload["stats"]
    ts = payload["run_time"]

    lines = [
        f"# 商品期货黑天鹅雷达 · {ts}",
        "",
        f"> 扫描 {stats['news']} 条快讯（{stats['sources']} 个源）／{stats['varieties']} 个品种｜"
        f"新增命中 {stats['new_alerts']} 条｜P0 {stats['l1']} · P1 {stats['l2']}｜"
        f"本次耗时 {stats['elapsed']}s",
        "",
    ]
    if stats["errors"]:
        lines += [f"> ⚠️ 数据源异常：{'; '.join(stats['errors'])}", ""]

    grouped: dict[str, list[dict[str, Any]]] = {"L1": [], "L2": [], "L3": []}
    for a in alerts:
        grouped.setdefault(a["level"], []).append(a)

    if not grouped["L1"] and not grouped["L2"]:
        lines += ["## ✅ 本周期未捕捉到黑天鹅级事件", "",
                  "消息面平稳。以下为仍在监控中的一般性产业消息。", ""]

    for lvl in ("L1", "L2", "L3"):
        rows = grouped.get(lvl) or []
        if not rows:
            continue
        title, desc = LEVEL_META[lvl]
        shown = rows if lvl != "L3" else rows[:8]
        lines += [f"## {title}（{len(rows)}）", f"_{desc}_", ""]
        for a in shown:
            chg = a.get("chg")
            move = f"｜盘面 {chg:+.2f}%" if isinstance(chg, (int, float)) else ""
            dlabel = DIR_LABEL.get(a.get("direction") or "neutral", "中性")
            if a.get("impact"):
                dlabel += f"·{a['impact']}"
            lines += [f"**{a['variety']}（{a['code']}）** [{dlabel}]{move} · {a['time']} · {a['source']} · 评分 {a['score']}",
                      "",
                      f"> {a['text']}",
                      ""]
            tags = []
            if a["critical"]:
                tags.append("强事件：" + "/".join(a["critical"]))
            if a["major"]:
                tags.append("变量：" + "/".join(a["major"]))
            if a["macro"]:
                tags.append("宏观：" + "/".join(a["macro"]))
            tags.append(f"命中：{a['hit_word']}（{'品种名' if a['hit_type'] == 'alias' else '产业链词'}）")
            lines += ["`" + " ｜ ".join(tags) + "`", ""]
        if lvl == "L3" and len(rows) > len(shown):
            lines += [f"_另有 {len(rows) - len(shown)} 条一般性消息已记录至当日流水，未展开。_", ""]

    if macro:
        lines += [f"## 🌍 全球宏观事件外溢（{len(macro)}）", ""]
        for m in macro:
            lines += [f"- **{m['time']}｜{m['category']}**：{m['text']}",
                      f"  → 可能波及板块：{'、'.join(m['sectors']) or '—'}", ""]

    if movers:
        lines += ["## 📊 盘面异动榜（|涨跌| ≥ "
                  f"{payload['cfg']['thresholds']['price_move_alert']}%）", "",
                  "| 品种 | 交易所 | 最新价 | 涨跌幅 |", "|---|---|---|---|"]
        for m in movers[:15]:
            lines.append(f"| {m['name']} | {m['exchange']} | {m['price']:.2f} | {m['chg']:+.2f}% |")
        lines.append("")

        # 异动归因线索
        attr = payload.get("mover_attr", [])
        if attr:
            lines += ["**异动归因线索**", ""]
            for m in attr:
                if not m.get("related"):
                    lines += [f"- {m['name']}（{m['chg']:+.2f}%）：本窗口未检索到相关消息，"
                              "可能为资金行为或未公开信息，建议叠加技术面判断。"]
                    continue
                lines += [f"- {m['name']}（{m['chg']:+.2f}%）："]
                for r in m["related"]:
                    lines.append(f"  - {r['time']} {r['source']}｜{r['text']}")
            lines += [""]

    lines += ["---", "",
              f"数据源：华尔街见闻／新浪财经 7×24／金十数据／东方财富行情 · 生成于 {ts}",
              "本报告为信息聚合，不构成投资建议。"]
    return "\n".join(lines)


DASH_CSS = """
*{box-sizing:border-box}
body{margin:0;background:#0d1017;color:#e8ecf2;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;font-size:14px;line-height:1.6}
.wrap{width:100%;max-width:1180px;margin:0 auto;padding:22px 18px 40px}
.head{display:flex;flex-wrap:wrap;align-items:baseline;gap:12px;border-bottom:1px solid #242c3a;padding-bottom:14px;margin-bottom:18px}
h1{font-size:20px;font-weight:600;margin:0;letter-spacing:.5px}
.tag{font-size:12px;padding:2px 9px;border-radius:11px;border:1px solid #2c3646;color:#8b97a9}
.tag.ok{border-color:#2fa97a;color:#2fa97a}
.tag.bad{border-color:#e5484d;color:#e5484d;background:#2a1416}
.time{font-size:12px;color:#8b97a9;margin-left:auto}
.kpis{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:22px}
.kpi{flex:1 1 120px;background:#161b24;border:1px solid #242c3a;border-radius:9px;padding:12px 14px}
.kpi .n{font-size:22px;font-weight:650;letter-spacing:.5px}
.kpi .l{font-size:11px;color:#8b97a9;margin-top:2px}
.kpi.crit .n{color:#e5484d}.kpi.warn .n{color:#f0a02a}.kpi.dim .n{color:#8b97a9}
h2{font-size:15px;font-weight:600;margin:26px 0 12px;display:flex;align-items:center;gap:8px}
h2::before{content:"";width:3px;height:14px;background:#3d7dff;border-radius:2px}
.flow{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px}
@media(max-width:900px){.flow{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:560px){.flow{grid-template-columns:1fr}}
.step{flex:1 1 170px;background:#161b24;border:1px solid #242c3a;border-radius:9px;padding:12px 13px;position:relative}
.step .t{font-size:12px;color:#6f7f96;letter-spacing:.5px;margin-bottom:5px}
.step .b{font-size:13px;line-height:1.5}
.step.acc{border-color:#2f4a78;background:#151d2b}
.arrow{align-self:center;color:#3b4759;font-size:16px;padding:0 2px}
.alert{background:#161b24;border:1px solid #242c3a;border-left:3px solid #e5484d;border-radius:9px;padding:13px 15px;margin-bottom:10px}
.alert.L2{border-left-color:#f0a02a}
.alert .top{display:flex;flex-wrap:wrap;gap:9px;align-items:baseline;margin-bottom:6px}
.vname{font-size:15px;font-weight:650}
.pill{font-size:12px;padding:1px 7px;border-radius:4px;background:#232a36;color:#a8b4c6}
.pill.up{background:#3a1c1e;color:#f5625e}.pill.down{background:#152c26;color:#2fa97a}
.meta{font-size:12px;color:#6f7f96}
.txt{font-size:13.5px;color:#c4cedd}
.tags{margin-top:8px;display:flex;flex-wrap:wrap;gap:6px}
h2 .sub{font-size:11.5px;color:#7d8a9c;font-weight:400;margin-left:6px}
.note{font-size:12px;line-height:1.6;color:#7b8798;margin:-4px 0 12px}
.dirtag{display:inline-block;font-size:11.5px;padding:2px 7px;border-radius:5px;border:1px solid;font-weight:600;letter-spacing:.3px}
.dirtag.bull{color:#ff7b7b;background:#2a1518;border-color:#5a2630}
.dirtag.bear{color:#4ade80;background:#0f2417;border-color:#1f4a2f}
.dirtag.neu{color:#8b98aa;background:#161b24;border-color:#2a3240}
.tg.newtg{background:#2a1518;color:#ff6b6b;border-color:#5a2630}
.tg{font-size:11.5px;color:#8b97a9;background:#1c222c;border:1px solid #2a3342;padding:2px 7px;border-radius:4px}
.bars{margin-top:6px}
.bar{display:flex;align-items:center;gap:10px;margin-bottom:7px}
.bar .nm{width:92px;font-size:13px;flex:0 0 92px}
.bar .track{flex:1;height:9px;background:#1b212b;border-radius:5px;position:relative;overflow:hidden}
.bar .fill{height:100%;border-radius:5px}
.bar .val{width:66px;text-align:right;font-size:12.5px;font-variant-numeric:tabular-nums;flex:0 0 66px}
.grid{display:flex;flex-wrap:wrap;gap:6px}
.cell{flex:0 0 calc(10% - 6px);min-width:74px;border-radius:7px;padding:7px 8px;border:1px solid #242c3a;background:#141922;cursor:pointer;transition:border-color .12s}
.cell:hover{border-color:#3d7dff}
.cell .c1{font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cell .c2{font-size:13px;font-weight:600;font-variant-numeric:tabular-nums}
.sect{font-size:12px;color:#6f7f96;margin:14px 0 7px;letter-spacing:1px}
.note{font-size:12px;color:#6f7f96;margin-top:6px}
.empty{background:#141a22;border:1px dashed #2a3342;border-radius:9px;padding:18px;text-align:center;color:#7d8a9c}
.foot{margin-top:30px;border-top:1px solid #242c3a;padding-top:12px;font-size:11.5px;color:#5f6c80}
"""


def _chg_color(chg: float | None) -> str:
    if not isinstance(chg, (int, float)):
        return "#2a3342"
    if chg >= 5:
        return "#b3261e"
    if chg >= 3:
        return "#8c2b22"
    if chg >= 1:
        return "#5c2d2a"
    if chg > 0:
        return "#3a2f2e"
    if chg > -1:
        return "#26382f"
    if chg > -3:
        return "#1e4a37"
    if chg > -5:
        return "#14603f"
    return "#0d7a4a"


DASH_EXTRA_CSS = """
.toolbar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:0 0 16px}
#q{flex:1 1 240px;background:#141922;border:1px solid #2a3342;border-radius:7px;padding:9px 13px;color:#e8ecf2;font-size:14px;outline:none}
#q:focus{border-color:#3d7dff}
#hint{font-size:12px;color:#6f7f96}
.kpi.clickable{cursor:pointer;transition:border-color .15s,transform .1s}
.kpi.clickable:hover{border-color:#3d7dff;transform:translateY(-1px)}
.nlink{color:#3d7dff;font-size:11px;margin-left:3px}
.mask{display:none;position:fixed;left:0;top:0;right:0;bottom:0;background:rgba(6,9,14,.74);z-index:9}
.panel{display:none;position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:860px;max-width:92vw;max-height:76vh;background:#121823;border:1px solid #2a3342;border-radius:11px;z-index:10;flex-direction:column;overflow:hidden;box-shadow:0 18px 50px rgba(0,0,0,.6)}
.phead{display:flex;gap:10px;align-items:center;padding:13px 15px;border-bottom:1px solid #242c3a;flex-wrap:wrap}
#ptitle{font-size:15px;font-weight:600;flex:1 1 auto}
#pq{flex:0 1 190px;background:#0e1420;border:1px solid #2a3342;border-radius:6px;padding:7px 10px;color:#e8ecf2;font-size:13px;outline:none}
#pq:focus{border-color:#3d7dff}
#pclose{background:#1c2430;border:1px solid #2a3342;color:#8b97a9;width:32px;height:32px;border-radius:6px;cursor:pointer;font-size:14px;line-height:1}
#pclose:hover{color:#e8ecf2;border-color:#3d7dff}
.pbody{overflow-y:auto;padding:2px 15px 18px}
.pf{display:flex;gap:6px;align-items:center;padding:10px 0 2px;font-size:12px;color:#8b98aa;flex-wrap:wrap}
.pbtn{background:#161b24;border:1px solid #2a3240;color:#b8c2cf;border-radius:6px;padding:5px 11px;font-size:12.5px;cursor:pointer;min-height:32px}
.pbtn.on{background:#1d2a3f;border-color:#3d7dff;color:#e6edf7}
.pitem{padding:10px 0;border-bottom:1px solid #1c2330}
.pt{font-size:11.5px;color:#6f7f96;margin-bottom:3px}
.px{font-size:13px;color:#c4cedd;line-height:1.6}
.pempty{padding:26px;text-align:center;color:#7d8a9c}
.sectwrap{margin-bottom:2px}
@media (max-width:760px){
  body{font-size:14.5px}
  .wrap{padding:14px 12px 28px}
  h1{font-size:17px}
  .step .b{font-size:14px}
  .step .t{font-size:12.5px}
  .sect{font-size:13px}
  .cell .c1{font-size:12.5px}
  .cell .c2{font-size:13.5px}
  .kpi .l{font-size:12px}
  .pt{font-size:12px}
  .px{font-size:13.5px}
  #hint{font-size:12px}
  #q,#pq{font-size:16px}
  .head{gap:8px;padding-bottom:10px;margin-bottom:14px}
  .time{margin-left:0;width:100%}
  .kpi{flex:1 1 calc(50% - 5px);padding:10px 12px}
  .kpi .n{font-size:19px}
  .flow{grid-template-columns:1fr}
  h2{font-size:14px;margin:20px 0 9px}
  .alert{padding:11px 12px}
  .vname{font-size:14px}
  .txt{font-size:13px}
  .bar .nm{width:66px;flex:0 0 66px;font-size:12.5px}
  .bar .val{width:58px;flex:0 0 58px;font-size:12px}
  .cell{flex:0 0 calc(25% - 6px)}
  #q{flex:1 1 100%}
  #hint{font-size:11.5px}
  .panel{width:94vw;max-height:84vh}
  .phead{padding:10px 12px}
  #pq{flex:1 1 100%;order:3}
  .pbody{padding:0 12px 14px}
  .pitem{padding:9px 0}
  .px{font-size:12.5px}
}
"""

DASH_JS = r"""
(function(){
  var raw=document.getElementById("raw");
  if(!raw){return;}
  var D;
  try{D=JSON.parse(raw.textContent);}catch(e){return;}
  var q=document.getElementById("q"), hint=document.getElementById("hint");
  if(!q||!hint){return;}
  function low(s){return (s||"").toLowerCase();}

  function apply(){
    var k=low(q.value.trim()), visA=0, visC=0;
    var items=document.querySelectorAll("[data-k]");
    for(var i=0;i<items.length;i++){
      var el=items[i];
      var hit=(!k||low(el.getAttribute("data-k")).indexOf(k)>=0);
      el.style.display=hit?"":"none";
      if(hit){ if(el.className.indexOf("alert")>=0){visA++;} else {visC++;} }
    }
    var sw=document.querySelectorAll(".sectwrap"), anySect=false;
    for(var j=0;j<sw.length;j++){
      var g=sw[j].querySelector(".grid"), any=false;
      for(var c=0;c<g.children.length;c++){if(g.children[c].style.display!=="none"){any=true;break;}}
      sw[j].style.display=any?"":"none";
      if(any){anySect=true;}
    }
    var h2s=document.getElementById("h2sector");
    if(h2s){h2s.style.display=anySect?"":"none";}
    var h2m=document.getElementById("h2mover"), anyBar=false;
    var bars=document.querySelectorAll(".bar");
    for(var b=0;b<bars.length;b++){if(bars[b].style.display!=="none"){anyBar=true;break;}}
    if(h2m){h2m.style.display=anyBar?"":"none";}
    var alerts=[].slice.call(document.querySelectorAll(".alert[data-k]"));
    var vis=alerts.filter(function(x){return x.style.display!=="none";}).length;
    var empty=document.getElementById("emptyAlert");
    if(empty){empty.style.display=(k&&vis===0)?"block":"none";}
    hint.textContent=k
      ? ("命中 "+visA+" 条告警 · "+visC+" 个盘面项")
      : ("共 "+visC+" 个盘面项 + "+visA+" 条告警，输入品种名即可筛选");
  }
  q.addEventListener("input",apply);
  apply();

  var mask=document.getElementById("mask"),
      panel=document.getElementById("panel"),
      ptitle=document.getElementById("ptitle"),
      pbody=document.getElementById("pbody"),
      pq=document.getElementById("pq");
  if(!mask||!panel||!ptitle||!pbody||!pq){return;}

  function esc(s){
    return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  }
  function item(meta,txt,k){
    return '<div class="pitem" data-k="'+esc(k)+'"><div class="pt">'+esc(meta)+
           '</div><div class="px">'+esc(txt)+'</div></div>';
  }

  function build(kind){
    var h="",i,x,n=0;
    if(kind==="news"){
      for(i=0;i<D.news.length;i++){x=D.news[i];
        h+=item(x.time+" · "+x.source,(x.variety?"【"+x.variety+"】":"")+x.text,
                (x.variety||"")+" "+x.text);n++;}
    }else if(kind==="l1"||kind==="l2"){
      var lv=kind==="l1"?"L1":"L2";
      for(i=0;i<D.alerts.length;i++){x=D.alerts[i];
        if(x.level!==lv){continue;}
        h+=item(x.time+" · "+x.source+" · 评分 "+x.score+" · "+(x.chg==null?"无报价":(x.chg>0?"+":"")+x.chg.toFixed(2)+"%"),
                x.variety+"("+x.code+")：" + x.text,
                x.variety+" "+x.code+" "+x.text);n++;}
      if(!n){h='<div class="pempty">本周期没有该级别告警。</div>';}
    }else if(kind==="movers"){
      for(i=0;i<D.movers.length;i++){x=D.movers[i];
        h+=item((x.chg>0?"+":"")+x.chg.toFixed(2)+"% · 最新价 "+x.price,x.name+"（"+x.code+"）",
                x.name+" "+x.code);n++;}
      if(!n){h='<div class="pempty">本周期没有涨跌幅超过 4% 的品种。</div>';}
    }else if(kind==="elapsed"){
      h='<div class="pitem"><div class="px">本次扫描耗时 '+D.stats.elapsed+
        ' 秒：采集行情 '+D.stats.varieties+' 个合约、快讯 '+D.stats.news+
        ' 条，经指纹去重、品种映射、事件分级后输出 '+D.alerts.length+
        ' 条告警（P0 '+D.stats.l1+' / P1 '+D.stats.l2+'）。<br><br>'+
        '服务每 30 分钟自动扫描一次，本页每 60 秒自动重新加载。</div></div>';n=1;
    }
    return h;
  }

  var HF={lv:"all",range:720};
  function histRows(){
    var now=Date.now()/1000,out=[],i,x;
    for(i=0;i<D.hist.length;i++){
      x=D.hist[i];
      if(HF.lv!=="all"&&x.level!==HF.lv){continue;}
      if(now-(x.ts||0)>HF.range*3600){continue;}
      out.push(x);
    }
    out.sort(function(a,b){return (b.ts||0)-(a.ts||0);});
    return out;
  }
  function fmtTs(ts){
    var d=new Date(ts*1000),p=function(v){return (v<10?"0":"")+v;};
    return p(d.getMonth()+1)+"-"+p(d.getDate())+" "+p(d.getHours())+":"+p(d.getMinutes());
  }
  function buildHist(){
    var h='<div class="pf">级别<button class="pbtn'+(HF.lv==="all"?" on":"")+'" data-lv="all">全部</button><button class="pbtn'+(HF.lv==="L1"?" on":"")+'" data-lv="L1">P0</button><button class="pbtn'+(HF.lv==="L2"?" on":"")+'" data-lv="L2">P1</button></div>';
    var rs=[[24,"24小时"],[72,"3天"],[168,"7天"],[720,"30天"]];
    h+='<div class="pf">时间';
    for(var i=0;i<rs.length;i++){h+='<button class="pbtn'+(HF.range===rs[i][0]?" on":"")+'" data-rg="'+rs[i][0]+'">'+rs[i][1]+'</button>';}
    h+='</div><div id="hlist"></div>';
    return h;
  }
  function renderHist(){
    var list=document.getElementById("hlist");
    if(!list){return;}
    var rows=histRows(),k=low(pq.value.trim()),h="",i,x,cnt=0;
    for(i=0;i<rows.length;i++){
      x=rows[i];
      var dk=(x.variety||"")+" "+(x.code||"")+" "+(x.text||"");
      if(k&&low(dk).indexOf(k)<0){continue;}
      cnt++;
      var dl={bull:"利多",bear:"利空",neutral:"中性"}, dc=x.direction||"neutral";
      h+='<div class="pitem" data-k="'+esc(dk)+'"><div class="pt"><span class="dirtag '+dc+'">'+(dl[dc]||"中性")+(x.impact?" · "+x.impact:"")+'</span> '+esc(fmtTs(x.ts||0))+' · '+(x.level==="L1"?"P0":"P1")+' · '+esc(x.variety||"")+
         (x.chg==null?"":" "+(x.chg>0?"+":"")+x.chg.toFixed(2)+"%")+' · '+esc(x.source||"")+'</div><div class="px">'+esc(x.text||"")+'</div></div>';
    }
    list.innerHTML=h||'<div class="pempty">没有匹配的告警。</div>';
    if(cnt){ptitle.textContent="告警历史 · "+cnt+" 条";}
  }
  var T={news:"本周期全部快讯",l1:"P0 历史明细",l2:"P1 历史明细",movers:"盘面异动明细",elapsed:"扫描耗时说明"};
  function openPanel(kind){
    if(kind==="hist"||kind==="l1"||kind==="l2"){
      HF.lv=(kind==="l1")?"L1":((kind==="l2")?"L2":"all");
      ptitle.textContent="告警历史 · 近 30 天";
      pbody.innerHTML=buildHist();renderHist();
      pq.value="";pq.style.display="";
      mask.style.display="block";panel.style.display="flex";
      return;
    }
    ptitle.textContent=(T[kind]||"")+(kind==="news"?" · "+D.news.length+" 条":"");
    pbody.innerHTML=build(kind);
    pq.value="";pq.style.display="";
    mask.style.display="block";
    panel.style.display="flex";
    pq.focus();
  }
  function closePanel(){mask.style.display="none";panel.style.display="none";}
  function spark(hist){
    if(!hist||hist.length<2){return "";}
    var w=340,h=76,pad=8,v=[],i,keep=[];
    for(i=0;i<hist.length;i++){if(hist[i][1]>0){keep.push(hist[i]);}}
    if(keep.length<2){return "";}
    hist=keep;
    for(i=0;i<hist.length;i++){v.push(hist[i][1]);}
    var mn=Math.min.apply(null,v),mx=Math.max.apply(null,v),rg=(mx-mn)||1,pts=[];
    for(i=0;i<hist.length;i++){
      var px=pad+i*(w-2*pad)/(hist.length-1);
      var py=h-pad-(hist[i][1]-mn)/rg*(h-2*pad);
      pts.push(px.toFixed(1)+","+py.toFixed(1));
    }
    var d=pts.join(" ");
    return '<svg viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none" style="width:100%;height:76px;display:block;margin:8px 0 2px">'+
      '<polyline fill="none" stroke="#3d7dff" stroke-opacity=".22" stroke-width="7" points="'+d+'"/>'+
      '<polyline fill="none" stroke="#3d7dff" stroke-width="2.2" points="'+d+'"/></svg>'+
      '<div class="px" style="color:#7d8a9c;font-size:11.5px">近 '+hist.length+' 期区间 '+mn.toLocaleString()+' ~ '+mx.toLocaleString()+'（单位同源数据，非港口/社会库存）</div>';
  }

  function openVar(code){
    var q=D.quotes[code]||{}, inv=D.inv[code], f=D.fund[code]||{}, h="", i, rel=[], nm=q.name||"";
    h+='<div class="pitem" style="border-top:none"><div class="px" style="font-size:13px;color:#8b98aa">'+esc(q.sector||"")+' · '+esc(q.exchange||"")+'</div>'+
       '<div style="margin-top:4px;font-size:21px;font-weight:700;color:'+(q.chg==null?'#8b98aa':(q.chg>0?'#ff7b72':'#3fb950'))+'">'+
       (q.chg==null?"无报价":((q.chg>0?"+":"")+q.chg.toFixed(2)+"%"))+
       (q.price?' <span style="font-size:13px;font-weight:400;color:#8b98aa">最新价 '+esc(q.price)+'</span>':'')+'</div></div>';
    if(inv){
      var lastD=inv.hist[inv.hist.length-1][0];
      h+='<div class="pitem"><div class="pt">库存 · '+(inv.freq||"日")+'频（'+esc(inv.src||"交易所仓单")+' 口径）</div>'+
         '<div class="px">最新 <b style="font-size:15px">'+inv.last.toLocaleString()+'</b> <span style="color:#8b98aa">'+lastD+'</span>'+
         '　增减 <b style="color:'+(inv.chg>0?'#ff7b72':(inv.chg<0?'#3fb950':'#8b98aa'))+'">'+(inv.chg>0?"+":"")+inv.chg.toLocaleString()+'</b></div>'+
         spark(inv.hist)+'</div>';
    }else{
      h+='<div class="pitem"><div class="pt">库存</div><div class="px" style="color:#7d8a9c">该品种暂无公开的日频库存口径。</div></div>';
    }
    var bs=D.bas[code];
    if(bs){
      var br=bs.rate*100, dir=bs.basis>0?"期货升水，现货相对宽松":"现货升水，现货偏紧";
      var col=Math.abs(br)<1?"#8b98aa":(bs.basis>0?"#3fb950":"#ff7b72");
      h+='<div class="pitem"><div class="pt">现货与基差 · 日频（'+esc(bs.contract)+' 主力）</div>'+
         '<div class="px">现货 <b>'+bs.spot.toLocaleString()+'</b>　基差 <b style="color:'+col+'">'+(bs.basis>0?"+":"")+bs.basis.toFixed(2)+'（'+(br>0?"+":"")+br.toFixed(2)+'%）</b></div>'+
         '<div class="px" style="color:'+col+';margin-top:4px">'+dir+'（基差＝期货－现货，负值越大越偏紧）</div></div>';
    }
    h+='<div class="pitem"><div class="pt">供给端看什么</div><div class="px">'+esc(f.supply||"暂无画像")+'</div></div>';
    h+='<div class="pitem"><div class="pt">需求端看什么</div><div class="px">'+esc(f.demand||"暂无画像")+'</div></div>';
    h+='<div class="pitem"><div class="pt">库存与信号</div><div class="px">'+esc(f.watch||"—")+'</div></div>';
    for(i=0;i<D.news.length;i++){
      var x=D.news[i];
      if(nm&&((x.variety&&x.variety.indexOf(nm)>=0)||x.text.indexOf(nm)>=0)){rel.push(x);}
    }
    h+='<div class="pitem" style="border-bottom:none"><div class="pt">本窗口相关快讯 · '+rel.length+' 条</div>';
    if(rel.length){
      for(i=0;i<rel.length&&i<8;i++){
        h+='<div class="px" style="margin:5px 0">· '+esc(rel[i].time)+' '+esc(rel[i].text)+'</div>';
      }
    }else{
      h+='<div class="px" style="color:#7d8a9c">本窗口没有与该品种直接相关的消息。</div>';
    }
    h+='</div>';
    ptitle.textContent=nm+"（"+code+"）";
    pbody.innerHTML=h;
    pq.value="";pq.style.display="none";
    mask.style.display="block";panel.style.display="flex";
  }

  document.getElementById("pclose").onclick=closePanel;
  mask.onclick=closePanel;
  document.addEventListener("keydown",function(e){if(e.key==="Escape"){closePanel();}});

  var kpis=document.querySelectorAll(".kpi[data-panel]");
  for(var i=0;i<kpis.length;i++){
    (function(el){el.onclick=function(){openPanel(el.getAttribute("data-panel"));};})(kpis[i]);
  }
  var hm=document.getElementById("hmore");
  if(hm){hm.onclick=function(e){e.preventDefault();openPanel("hist");};}
  var vcells=document.querySelectorAll(".cell[data-v]");
  for(var ci=0;ci<vcells.length;ci++){
    (function(el){el.onclick=function(){openVar(el.getAttribute("data-v"));};})(vcells[ci]);
  }
  pbody.addEventListener("click",function(e){
    var t=e.target;
    if(!t||!t.getAttribute){return;}
    var lv=t.getAttribute("data-lv"),rg=t.getAttribute("data-rg");
    if(lv){HF.lv=lv;}else if(rg){HF.range=parseInt(rg,10);}else{return;}
    pbody.innerHTML=buildHist();renderHist();
  });
  pq.addEventListener("input",function(){
    if(document.getElementById("hlist")){renderHist();return;}
    var k=low(pq.value.trim());
    var its=pbody.querySelectorAll(".pitem");
    for(var j=0;j<its.length;j++){
      its[j].style.display=(!k||low(its[j].getAttribute("data-k")).indexOf(k)>=0)?"":"none";
    }
  });
})();
"""


DIR_LABEL = {"bull": "利多", "bear": "利空", "neutral": "中性"}


def dir_tag(a: dict[str, Any]) -> str:
    """渲染利多/利空 + 影响强度标签。"""
    d = a.get("direction") or "neutral"
    imp = a.get("impact") or ""
    cls = {"bull": "dirtag bull", "bear": "dirtag bear"}.get(d, "dirtag neu")
    txt = DIR_LABEL.get(d, "中性") + (f" · {imp}" if imp else "")
    basis = "/".join(a.get("dir_basis") or [])
    tip = f' title="依据：{basis}"' if basis else ""
    return f'<span class="{cls}"{tip}>{txt}</span>'


AUTH_TOKEN = "r4d4r-auth-v1"
AUTH_FILE = Path(__file__).resolve().parent / "accounts.json"


def _auth_hash(v: str) -> str:
    x = 5381
    for ch in v:
        x = ((x * 33) ^ ord(ch)) & 0xFFFFFFFF
    return format(x, "x")


def _load_auth_accounts() -> list:
    try:
        rows = [(str(a["user"]), _auth_hash(str(a["user"]) + "|" + str(a["pass"])))
                for a in json.loads(AUTH_FILE.read_text(encoding="utf-8"))
                if a.get("user") and a.get("pass")]
        if rows:
            return rows
    except Exception:
        pass
    # 无 accounts.json 时（如 GitHub Actions 无明文配置）回退到内置哈希表
    return [("doubao", "b67d3177"), ("viewer", "6a80eb52"),
            ("guest", "18e2ac39"), ("admin", "fc111c71")]


AUTH_ACCOUNTS = _load_auth_accounts()

GATE_JS = """
(function(){
var A=[__ACCTS__];
var T="__TOKEN__";
function H(s){var x=5381;for(var i=0;i<s.length;i++){x=((x*33)^s.charCodeAt(i))>>>0;}return x.toString(16);}
var g=document.getElementById("gate"),ap=document.getElementById("app");
if(document.documentElement.className==="authed"){if(g&&g.parentNode)g.parentNode.removeChild(g);if(ap)ap.style.display="";return;}
function ok(){try{localStorage.setItem("radar_auth",T)}catch(e){};document.documentElement.className="authed";if(g&&g.parentNode)g.parentNode.removeChild(g);if(ap)ap.style.display="";}
function go(){var u=(document.getElementById("gu").value||"").trim(),p=document.getElementById("gp").value||"";var hh=H(u+"|"+p);for(var i=0;i<A.length;i++){if(A[i][0]===u&&A[i][1]===hh){ok();return;}}document.getElementById("gerr").textContent="账号或密码不正确";}
var b=document.getElementById("gb"),pu=document.getElementById("gu"),pp=document.getElementById("gp");
b.onclick=go;
pp.onkeydown=function(e){if(e.key==="Enter"){go();}};
pu.onkeydown=function(e){if(e.key==="Enter"){pp.focus();}};
})();
"""


def _gate_js() -> str:
    rows = ",".join('["%s","%s"]' % (u, hh) for u, hh in AUTH_ACCOUNTS)
    return GATE_JS.replace("__ACCTS__", rows).replace("__TOKEN__", AUTH_TOKEN)

def render_dashboard(payload: dict[str, Any], news: list[dict[str, Any]] | None = None,
                     inventory: dict[str, Any] | None = None,
                     fundamentals: dict[str, Any] | None = None,
                     basis: dict[str, Any] | None = None,
                     rolling: list[dict[str, Any]] | None = None) -> str:
    """生成自包含的本地监控台（可搜索品种、明细可点开，离线可用）。"""
    st = payload["stats"]
    alerts = payload["alerts"]
    movers = payload["movers"]
    quotes = payload["quotes"]
    macro = payload["macro"]
    ts = payload["run_time"]
    news_view = news or []

    l1 = [a for a in alerts if a["level"] == "L1"]
    l2 = [a for a in alerts if a["level"] == "L2"]
    status_tag = (f'<span class="tag bad">● {len(l1)} 起 P0 突发</span>' if l1
                  else f'<span class="tag warn">● {len(l2)} 起 P1 关注</span>' if l2
                  else '<span class="tag ok">● 正常 · 无黑天鹅</span>')

    parts = [f'''<div class="wrap">
<div class="head">
  <h1>商品期货黑天鹅雷达</h1>
  {status_tag}
  <span class="tag">覆盖 {st["varieties"]} 个品种</span>
  <span class="time">最近扫描 {ts}</span>
</div>

<div class="toolbar">
  <input id="q" type="search" placeholder="搜索品种：铁矿石 / 原油 / 苹果 / SC / AP …" autocomplete="off">
  <span id="hint">加载中…</span>
</div>

<div class="kpis">
  <div class="kpi clickable" data-panel="news"><div class="n">{st["news"]}</div><div class="l">本周期快讯<span class="nlink">›</span></div></div>
  <div class="kpi crit clickable" data-panel="l1"><div class="n">{len(l1)}</div><div class="l">P0 突发<span class="nlink">›</span></div></div>
  <div class="kpi warn clickable" data-panel="l2"><div class="n">{len(l2)}</div><div class="l">P1 重要<span class="nlink">›</span></div></div>
  <div class="kpi clickable" data-panel="movers"><div class="n">{len(movers)}</div><div class="l">盘面异动 ≥4%<span class="nlink">›</span></div></div>
  <div class="kpi dim clickable" data-panel="elapsed"><div class="n">{st["elapsed"]}s</div><div class="l">扫描耗时<span class="nlink">›</span></div></div>
</div>

<h2>数据管线</h2>
<div class="flow">
  <div class="step"><div class="t">① 采集</div><div class="b">华尔街见闻（全球/商品）+ 新浪 7×24 + 金十数据<br><span style="color:#6f7f96">新浪期货行情（主）/ 东财（兜底）</span></div></div>
  <div class="step"><div class="t">② 归一化</div><div class="b">统一时间口径、内容指纹去重<br><span style="color:#6f7f96">同一事件跨源只计一次</span></div></div>
  <div class="step acc"><div class="t">③ 品种映射</div><div class="b">79 品种 × 别名 + 强产业链词<br><span style="color:#6f7f96">排除同名公司语境（果链≠苹果期货）</span></div></div>
  <div class="step acc"><div class="t">④ 事件分级</div><div class="b">供应中断/制裁/灾害级 → L1<br>变量变化 → L2，常规进度 → L3</div></div>
  <div class="step"><div class="t">⑤ 输出</div><div class="b">报告 + 本地看板 + webhook<br><span style="color:#6f7f96">叠加盘面异动二次确认</span></div></div>
</div>
''']

    # 告警区：近 48 小时（数据来自 30 天滚动库）
    now_ts = time.time()
    recent = [r for r in (rolling or []) if now_ts - float(r.get("last_ts") or 0) < 48 * 3600]
    recent.sort(key=lambda r: (0 if r.get("level") == "L1" else 1, -float(r.get("ts") or 0)))
    n_l1 = sum(1 for r in recent if r.get("level") == "L1")
    n_l2 = sum(1 for r in recent if r.get("level") == "L2")
    n_new = sum(1 for r in recent if now_ts - float(r.get("first_ts") or r.get("ts") or 0) < 2400)
    parts.append('<h2>黑天鹅告警</h2>')
    parts.append(f'<div class="sub" style="margin:-6px 0 8px">近 48 小时 · P0 {n_l1} · P1 {n_l2}｜本周期新增 {n_new} 条 · <a href="javascript:void(0)" id="hmore" style="color:#3d7dff;text-decoration:none">查 30 天历史 ›</a></div>')
    parts.append('<div class="note">方向按事件性质规则推断（供应中断/灾害＝利多，复产/累库/需求走弱＝利空），'
                 '强度看是否属停产断供级硬冲击。仅供参考，下单前请自行复核原文。</div>')
    if not recent:
        parts.append('<div class="empty">近 48 小时没有黑天鹅级事件，消息面平稳。</div>')
    for a in recent[:12]:
        cls = "alert L2" if a["level"] == "L2" else "alert"
        chg = a.get("chg")
        pill = (f'<span class="pill {"up" if chg > 0 else "down"}">{chg:+.2f}%</span>'
                if isinstance(chg, (int, float)) else '<span class="pill">无报价</span>')
        tags = "".join(f'<span class="tg">{t}</span>' for t in
                       ([f"强事件 {'/'.join(a['critical'])}"] if a["critical"] else [])
                       + ([f"变量 {'/'.join(a['major'])}"] if a["major"] else [])
                       + [f"命中 {a['hit_word']}"])
        fts = float(a.get("first_ts") or a.get("ts") or 0)
        show_time = datetime.fromtimestamp(fts).strftime("%m-%d %H:%M") if fts else str(a.get("time", ""))
        fresh = (now_ts - fts) < 2400
        newtag = ('<span class="tg newtg">NEW</span>' if fresh else '')
        dirtag = dir_tag(a)
        dk = f"{a['variety']} {a['code']} {a.get('sector','')} {a['text'][:90]}"
        parts.append(f'''<div class="{cls}" data-k="{dk}">
  <div class="top"><span class="vname">{a["variety"]} {a["code"]}</span>{pill}
    <span class="meta">{show_time} · {a["source"]} · {'P0' if a["level"] == "L1" else "P1"} · 评分 {a["score"]}</span></div>
  <div class="txt">{a["text"]}</div>
  <div class="tags">{newtag}{dirtag}{tags}</div>
</div>''')
    parts.append('<div class="empty" id="emptyAlert" style="display:none">该品种近 48 小时没有告警。</div>')

    # 盘面异动
    parts.append('<h2 id="h2mover">盘面异动</h2>')
    if movers:
        parts.append('<div class="bars">')
        for m in movers[:12]:
            w = min(abs(m["chg"]) / 8 * 100, 100)
            color = "#e5484d" if m["chg"] > 0 else "#2fa97a"
            parts.append(f'''<div class="bar" data-k="{m["name"]} {m["code"]} {m["chg"]:+.2f}">
<span class="nm">{m["name"]}</span>
<div class="track"><div class="fill" style="width:{w:.0f}%;background:{color}"></div></div>
<span class="val" style="color:{color}">{m["chg"]:+.2f}%</span></div>''')
        parts.append('</div>')
        for m in payload.get("mover_attr") or []:
            if m.get("related"):
                parts.append(f'<div class="note" data-k="{m["name"]} {m["code"]}">▪ {m["name"]} 归因：'
                             + "；".join(f'{r["time"]} {r["text"][:36]}' for r in m["related"][:2]) + '</div>')
            else:
                parts.append(f'<div class="note" data-k="{m["name"]} {m["code"]}">▪ {m["name"]}：本窗口无相关消息，注意资金行为或更早驱动</div>')
    else:
        parts.append('<div class="empty">无品种涨跌幅超过 4%。</div>')

    # 全品种热力
    parts.append('<h2 id="h2sector">全品种盘面</h2>')
    SECTOR_ORDER = ["黑色", "有色", "贵金属", "能化", "农产品", "新能源", "航运"]
    by_sector: dict[str, list[dict[str, Any]]] = {}
    for q in quotes.values():
        by_sector.setdefault(q["sector"] or "其他", []).append(q)
    ordered = sorted(by_sector.items(),
                     key=lambda kv: (SECTOR_ORDER.index(kv[0]) if kv[0] in SECTOR_ORDER else 99))
    for sector, items in ordered:
        parts.append(f'<div class="sectwrap"><div class="sect">{sector} · {len(items)}</div><div class="grid">')
        for q in sorted(items, key=lambda x: -(x.get("chg") or 0)):
            bg = _chg_color(q.get("chg"))
            na = " 无报价" if q.get("chg") is None else ""
            parts.append(f'''<div class="cell" style="background:{bg}" data-v="{q["code"]}" data-k="{q["name"]} {q["code"]} {sector} {q["exchange"]}">
<div class="c1">{q["name"]}</div><div class="c2">{q["chg"]:+.2f}%</div></div>''')
        parts.append('</div></div>')

    if macro:
        parts.append('<h2>全球宏观外溢</h2>')
        for m in macro:
            parts.append(f'<div class="alert L2"><div class="top"><span class="vname" style="font-size:13.5px">'
                         f'[{m["category"]}]</span><span class="meta">{m["time"]} · {m["source"]}</span></div>'
                         f'<div class="txt">{m["text"]}</div>'
                         f'<div class="tags"><span class="tg">波及板块 {"/".join(m["sectors"])}</span></div></div>')

    parts.append(f'''<div class="foot">
数据源：华尔街见闻 · 新浪财经 7×24 · 金十数据 · 新浪期货行情／东方财富兜底｜
扫描窗口 65 分钟，告警留存 30 天可查，每 30 分钟自动刷新｜本页由 radar.py 生成，仅供信息参考，不构成投资建议。<br>
<span style="color:#7d8a9c">自动刷新：<span id="cd">60</span> 秒后重新加载</span>
<a href="javascript:location.reload()" style="color:#3d7dff;text-decoration:none;margin-left:10px">立即刷新</a>
</div></div>

<div class="mask" id="mask"></div>
<div class="panel" id="panel">
  <div class="phead">
    <span id="ptitle"></span>
    <input id="pq" type="search" placeholder="在列表中筛选…" autocomplete="off">
    <button id="pclose" title="关闭">✕</button>
  </div>
  <div class="pbody" id="pbody"></div>
</div>''')

    raw = json.dumps({
        "news": news_view,
        "alerts": [{"level": a["level"], "variety": a["variety"], "code": a["code"],
                    "time": a["time"], "source": a["source"], "text": a["text"],
                    "score": a["score"], "chg": a.get("chg")} for a in alerts],
        "movers": [{"name": m["name"], "code": m["code"], "chg": m["chg"],
                    "price": m.get("price")} for m in movers],
        "stats": st,
        "quotes": {c: {"name": q.get("name"), "chg": q.get("chg"),
                       "price": q.get("price"), "sector": q.get("sector"),
                       "exchange": q.get("exchange")} for c, q in quotes.items()},
        "inv": inventory or {},
        "fund": fundamentals or {},
        "bas": basis or {},
        "hist": [{"ts": r.get("ts"), "level": r.get("level"), "variety": r.get("variety"),
                  "code": r.get("code"), "source": r.get("source"), "text": r.get("text"),
                  "score": r.get("score"), "chg": r.get("chg"),
                  "direction": r.get("direction"), "impact": r.get("impact")}
                 for r in (rolling or [])],
    }, ensure_ascii=False).replace("</", "<\\/")

    gate_html = (
        "<div id=\"gate\" style=\"position:fixed;inset:0;z-index:9999;background:#0b0f16;"
        "display:flex;align-items:center;justify-content:center;padding:20px\">"
        "<div style=\"width:330px;max-width:100%;background:#111823;border:1px solid #223047;"
        "border-radius:12px;padding:26px 24px\">"
        "<div style=\"color:#e8eef7;font-size:18px;font-weight:600\">商品期货黑天鹅雷达</div>"
        "<div style=\"color:#7d8ea3;font-size:12px;margin:6px 0 18px\">仅限授权账号访问</div>"
        "<input id=\"gu\" placeholder=\"账号\" autocomplete=\"username\" style=\"width:100%;box-sizing:border-box;"
        "background:#0b1119;border:1px solid #223047;color:#e8eef7;border-radius:8px;padding:10px 12px;"
        "font-size:14px;margin-bottom:10px;outline:none\">"
        "<input id=\"gp\" type=\"password\" placeholder=\"密码\" autocomplete=\"current-password\" style=\"width:100%;"
        "box-sizing:border-box;background:#0b1119;border:1px solid #223047;color:#e8eef7;border-radius:8px;"
        "padding:10px 12px;font-size:14px;margin-bottom:14px;outline:none\">"
        "<button id=\"gb\" style=\"width:100%;background:#2f6fd0;color:#fff;border:0;border-radius:8px;"
        "padding:11px;font-size:14px;font-weight:600;cursor:pointer\">登录</button>"
        "<div id=\"gerr\" style=\"color:#ff5a5a;font-size:12px;height:18px;margin-top:8px\"></div>"
        "</div></div>")

    return ("<!DOCTYPE html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
            "<meta http-equiv=\"refresh\" content=\"60\">"
            f"<title>商品期货黑天鹅雷达</title><style>{DASH_CSS}{DASH_EXTRA_CSS}</style>"
            "<style>html.authed #gate{display:none!important}html.authed #app{display:block!important}</style>"
            "<script>(function(){try{if(localStorage.getItem('radar_auth')==='" + AUTH_TOKEN + "')"
            "{document.documentElement.className='authed'}}catch(e){}})()</script></head>"
            f"<body>{gate_html}<div id=\"app\" style=\"display:none\">"
            f"{''.join(parts)}"
            f"<script id=\"raw\" type=\"application/json\">{raw}</script>"
            f"<script>{DASH_JS}</script></div>"
            f"<script>{_gate_js()}</script></body></html>")


INVENTORY_PATH = STATE_DIR / "inventory.json"


def fetch_inventory(cfg: dict[str, Any], ttl_hours: float = 6.0) -> dict[str, Any]:
    """抓各品种日频库存（东方财富交易所库存口径），本地缓存，失败静默降级。"""
    try:
        cache = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        if time.time() - float(cache.get("ts", 0)) < ttl_hours * 3600:
            return cache.get("data", {})
    except Exception:                       # noqa: BLE001 无缓存或损坏就重抓
        pass
    data: dict[str, Any] = {}
    try:
        from concurrent.futures import ThreadPoolExecutor
        import akshare as ak
        from akshare.futures.futures_inventory_em import futures_inventory_em_symbol_dict as DICT
    except Exception as exc:                # noqa: BLE001 未装 akshare 时跳过库存
        log(f"[库存] akshare 不可用，跳过库存：{exc}")
        return data
    code2cn = {str(v).strip().upper(): k for k, v in DICT.items() if v}
    targets = []
    for v in cfg["varieties"]:
        root = re.sub(r"[mM]$", "", v["code"]).upper()
        cn = code2cn.get(root)
        if cn:
            targets.append((v["code"], cn))

    def one(t: tuple[str, str]):
        code, cn = t
        try:
            df = ak.futures_inventory_em(symbol=cn).tail(60)
            hist = [[str(r["日期"])[:10], float(r["库存"])] for _, r in df.iterrows()]
            if not hist:
                return code, None
            return code, {"last": hist[-1][1], "chg": float(df.iloc[-1]["增减"]),
                          "hist": hist, "src": "东财仓单"}
        except Exception:                   # noqa: BLE001 单品种失败不影响其他
            return code, None

    with ThreadPoolExecutor(max_workers=8) as ex:
        for code, d in ex.map(one, targets):
            if d:
                data[code] = d

    # 东财没有口径的品种，用 99 期货网周频库存补齐（原油、燃料油等）
    def one99(t: tuple[str, str]):
        code, name = t
        try:
            df = ak.futures_inventory_99(symbol=name).tail(60)
            hist = [[str(r["日期"])[:10], float(r["库存"]),
                     float(r["收盘价"])] for _, r in df.iterrows() if float(r["库存"]) > 0]
            if len(hist) < 2:
                return code, None
            return code, {"last": hist[-1][1], "chg": hist[-1][1] - hist[-2][1],
                          "hist": [[h[0], h[1]] for h in hist], "src": "99期货",
                          "freq": "周"}
        except Exception:                   # noqa: BLE001
            return code, None

    miss = [(v["code"], v["name"]) for v in cfg["varieties"] if v["code"] not in data]
    if miss:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for code, d in ex.map(one99, miss):
                if d:
                    data[code] = d
    if data:
        try:
            INVENTORY_PATH.write_text(json.dumps(
                {"ts": time.time(), "data": data}, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:            # noqa: BLE001
            log(f"[库存] 缓存写入失败：{exc}")
    log(f"[库存] 已更新 {len(data)}/{len(cfg['varieties'])} 个品种")
    return data


BASIS_PATH = STATE_DIR / "basis.json"


def fetch_basis(cfg: dict[str, Any], ttl_hours: float = 6.0) -> dict[str, Any]:
    """抓现货价与主力基差（日频），基差反映现货松紧，本地缓存。"""
    try:
        cache = json.loads(BASIS_PATH.read_text(encoding="utf-8"))
        if time.time() - float(cache.get("ts", 0)) < ttl_hours * 3600:
            return cache.get("data", {})
    except Exception:                       # noqa: BLE001
        pass
    data: dict[str, Any] = {}
    try:
        import akshare as ak
    except Exception as exc:                # noqa: BLE001
        log(f"[基差] akshare 不可用，跳过：{exc}")
        return data
    root2code: dict[str, str] = {}
    for v in cfg["varieties"]:
        root2code.setdefault(re.sub(r"[mM]$", "", v["code"]).upper(), v["code"])
    codes = list(root2code.keys())
    df = None
    for back in range(0, 7):
        d = (datetime.now() - timedelta(days=back)).strftime("%Y%m%d")
        try:
            df = ak.futures_spot_price(date=d, vars_list=codes)
            if df is not None and len(df):
                break
        except Exception:                   # noqa: BLE001 非交易日或源抖动，往前找
            df = None
    if df is not None and len(df):
        for _, r in df.iterrows():
            sym = str(r["symbol"]).upper()
            code = root2code.get(sym)
            if not code:
                continue
            try:
                data[code] = {"spot": float(r["spot_price"]),
                              "basis": float(r["dom_basis"]),
                              "rate": float(r["dom_basis_rate"]),
                              "contract": str(r["dominant_contract"])}
            except Exception:               # noqa: BLE001 单行脏数据跳过
                continue
    if data:
        try:
            BASIS_PATH.write_text(json.dumps(
                {"ts": time.time(), "data": data}, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:            # noqa: BLE001
            log(f"[基差] 缓存写入失败：{exc}")
    log(f"[基差] 已更新 {len(data)} 个品种")
    return data


def _similar(x: str, y: str, ratio: float = 0.6) -> bool:
    """粗判两条快讯是否同一事件（跨源转载常加不同前缀）。"""
    import difflib
    if not x or not y:
        return False
    pat = r"[【】\s“”\"\'（）()，,。.、：:；;\-]"
    xs = re.sub(pat, "", x)
    ys = re.sub(pat, "", y)
    if not xs or not ys:
        return False
    if xs in ys or ys in xs:
        return True
    # 转载常在正文前加标题：用对方正文开头的 22 字做包含检测
    if len(ys) > 22 and ys[:22] in xs:
        return True
    if len(xs) > 22 and xs[:22] in ys:
        return True
    return difflib.SequenceMatcher(None, xs[:200], ys[:200]).ratio() > ratio


ROLLING_PATH = STATE_DIR / "alerts_rolling.json"


def merge_rolling(alerts: list[dict[str, Any]], keep_hours: float = 720.0) -> list[dict[str, Any]]:
    """把本周期告警并入滚动历史，靠 (品种+正文) 去重，保留 keep_hours 小时。"""
    try:
        old = json.loads(ROLLING_PATH.read_text(encoding="utf-8"))
        if not isinstance(old, list):
            old = []
    except Exception:                       # noqa: BLE001 首次运行或损坏
        old = []
    now = time.time()
    out = [dict(a) for a in old if a.get("last_ts") and now - float(a["last_ts"]) < keep_hours * 3600]
    idx: dict[str, int] = {}
    for i, a in enumerate(out):
        idx[f"{a.get('variety')}|{str(a.get('text'))[:80]}"] = i
    cfg = load_config()
    for a in alerts:
        if a.get("level") not in ("L1", "L2"):
            continue                        # L3 常规进度不进告警历史
        if not a.get("direction"):
            a["direction"], a["impact"], a["dir_basis"] = detect_direction(
                str(a.get("text") or ""), cfg)
        k = f"{a.get('variety')}|{str(a.get('text'))[:80]}"
        ts = a.get("ts") or now
        if k in idx:
            out[idx[k]]["last_ts"] = max(float(out[idx[k]].get("last_ts", 0)), ts)
            out[idx[k]]["level"] = a.get("level", out[idx[k]].get("level"))
            out[idx[k]]["score"] = max(float(out[idx[k]].get("score", 0) or 0),
                                       float(a.get("score") or 0))
            out[idx[k]]["direction"] = a.get("direction", out[idx[k]].get("direction"))
            out[idx[k]]["impact"] = a.get("impact", out[idx[k]].get("impact"))
            out[idx[k]]["dir_basis"] = a.get("dir_basis", out[idx[k]].get("dir_basis"))
            continue
        # 同品种高度相似的文本视为同一事件的不同源报道，不重复入库
        near = None
        for r in out:
            if r.get("variety") == a.get("variety") and _similar(
                    str(r.get("text") or ""), str(a.get("text") or "")):
                near = r
                break
        if near:
            near["last_ts"] = max(float(near.get("last_ts", 0)), ts)
            near["first_ts"] = min(float(near.get("first_ts") or ts), ts)
            if a.get("level") == "L1":
                near["level"] = "L1"          # 同一事件取更高级别
            if not near.get("direction"):
                near["direction"] = a.get("direction")
                near["impact"] = a.get("impact")
                near["dir_basis"] = a.get("dir_basis")
            if len(str(a.get("text") or "")) > len(str(near.get("text") or "")):
                near["text"] = a.get("text")
            continue
        rec = {"ts": ts, "last_ts": ts, "first_ts": ts, "first_run": a.get("first_run") or "",
               "level": a.get("level"), "variety": a.get("variety"), "code": a.get("code"),
               "time": a.get("time"), "source": a.get("source"), "text": a.get("text"),
               "score": a.get("score"), "chg": a.get("chg"), "sector": a.get("sector"),
               "critical": a.get("critical"), "major": a.get("major"),
               "hit_word": a.get("hit_word"),
               "direction": a.get("direction"), "impact": a.get("impact"),
               "dir_basis": a.get("dir_basis")}
        idx[k] = len(out)
        out.append(rec)
    out = out[-500:]
    try:
        ROLLING_PATH.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:                # noqa: BLE001
        log(f"[滚动] 写入失败：{exc}")
    return out


def publish_dashboard(cfg: dict[str, Any]) -> None:
    """把最新看板复发布到同一个 doubao-html 应用，手机固定链接即时更新。"""
    pub = cfg.get("publish", {})
    if not pub.get("enabled") or not pub.get("app_id"):
        return
    cli = pub.get("cli") or "lark-cli"
    import os
    import shutil
    # 首选 GitHub Pages（长期 token，不依赖用户登录态）
    gh_script = Path(__file__).resolve().parent / "publish_github.sh"
    gh_repo = Path(__file__).resolve().parent / "gh-pages" / ".git"
    if gh_script.exists() and gh_repo.exists():
        try:
            import subprocess
            r = subprocess.run(["bash", str(gh_script)], capture_output=True, text=True, timeout=120)
            if r.returncode == 0:
                log("[发布] GitHub Pages 已更新")
                return
            log(f"[发布] GitHub 失败 rc={r.returncode}: {(r.stderr or r.stdout)[:120]}")
        except Exception as exc:             # noqa: BLE001
            log(f"[发布] GitHub 异常：{exc}")

    if not (os.path.isabs(cli) and os.path.exists(cli)) and not shutil.which(cli):
        return                               # 换机器后没有 lark-cli，静默跳过
    try:
        import subprocess
        r = subprocess.run(
            [cli, "apps", "+deploy", "--file-path", "./dashboard.html",
             "--app-id", pub["app_id"], "--as", "user"],
            cwd=str(ALERT_DIR), timeout=90, capture_output=True, text=True)
        if r.returncode == 0:
            log(f"[发布] 手机看板已更新 → {pub.get('url', '')}")
        else:
            log(f"[发布] 失败 rc={r.returncode}: {(r.stderr or r.stdout)[:160]}")
    except Exception as exc:                 # noqa: BLE001 发布失败不影响扫描
        log(f"[发布] 异常：{exc}")


def notify_desktop(title: str, message: str) -> None:
    """跨平台桌面通知：Windows 弹窗 / macOS 通知中心 / Linux notify-send。"""
    try:
        if sys.platform == "win32":
            import ctypes
            import threading
            MB_ICONWARNING, MB_TOPMOST = 0x30, 0x4000
            # 放到子线程，避免弹窗阻塞扫描进程
            threading.Thread(
                target=lambda: ctypes.windll.user32.MessageBoxW(
                    0, message, title, MB_ICONWARNING | MB_TOPMOST),
                daemon=True).start()
            try:
                import winsound
                winsound.MessageBeep()
            except Exception:               # noqa: BLE001 部分环境无声卡
                pass
        elif sys.platform == "darwin":
            import subprocess
            subprocess.run(
                ["osascript", "-e",
                 f'display notification "{message}" with title "{title}"'],
                timeout=5, check=False)
        else:
            import shutil
            import subprocess
            if shutil.which("notify-send"):
                subprocess.run(["notify-send", title, message], timeout=5, check=False)
    except Exception as exc:                # noqa: BLE001 通知失败不影响主流程
        log(f"[通知] 桌面通知失败: {exc}")


def notify_webhook(cfg: dict[str, Any], text: str) -> bool:
    """把告警推送到手机。支持飞书/钉钉/企业微信/Server酱/PushPlus/Bark。"""
    wh = cfg.get("webhook", {})
    if not wh.get("enabled") or not wh.get("url"):
        return False
    kind = (wh.get("type") or "feishu").lower()
    url = wh["url"].strip()
    title = wh.get("title") or "商品黑天鹅雷达"
    body = text[:3000]
    try:
        if kind == "feishu":
            r = requests.post(url, json={"msg_type": "text", "content": {"text": body}}, timeout=10)
        elif kind == "dingtalk":
            r = requests.post(url, json={"msgtype": "text", "text": {"content": body}}, timeout=10)
        elif kind in ("wecom", "qywx", "qiwei"):
            r = requests.post(url, json={"msgtype": "text", "text": {"content": body}}, timeout=10)
        elif kind in ("serverchan", "sct", "方糖"):
            r = requests.post(url, data={"title": title, "desp": body}, timeout=10)
        elif kind == "pushplus":
            r = requests.post(url, json={"title": title, "content": body,
                                         "template": "markdown"}, timeout=10)
        elif kind == "bark":
            r = requests.post(url, json={"title": title, "body": body[:800],
                                         "group": "商品雷达"}, timeout=10)
        else:
            r = requests.post(url, json={"text": body}, timeout=10)

        ok = r.status_code < 300
        try:
            data = r.json()
            code = data.get("code", data.get("errcode", 0))
            if code not in (0, "0", None):
                ok = False
                log(f"[推送] 平台拒绝：{str(data)[:200]}")
        except ValueError:
            pass
        log(f"[推送] {kind} " + ("成功" if ok else f"失败 HTTP {r.status_code} {r.text[:160]}"))
        return ok
    except Exception as exc:                 # noqa: BLE001 推送失败不影响扫描
        log(f"[推送] 异常：{exc}")
        return False


def test_webhook(cfg: dict[str, Any]) -> int:
    """发一条测试消息，验证推送配置是否可用。"""
    wh = cfg.get("webhook", {})
    if not wh.get("url"):
        print("config.json 的 webhook.url 还是空的，请先填入机器人地址。")
        return 1
    probe = {**cfg, "webhook": {**wh, "enabled": True}}
    stamp = datetime.now(TZ8).strftime("%Y-%m-%d %H:%M:%S")
    ok = notify_webhook(probe, f"【测试】商品黑天鹅雷达已接入推送。\n时间 {stamp}\n"
                               f"收到这条消息说明配置成功，之后出现 P0/P1 会推到这里。")
    print("推送测试" + ("成功，去手机上查收。" if ok else "失败，请检查地址或关键词校验设置（详见日志）。"))
    return 0 if ok else 1


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def run_once(cfg: dict[str, Any], idx: dict[str, Any], first_run: bool) -> dict[str, Any]:
    t0 = time.time()
    state = load_state()
    window = FIRST_RUN_LOOKBACK_MIN if first_run else LOOKBACK_MIN
    cutoff = datetime.now() - timedelta(minutes=window)

    quotes = fetch_quotes(cfg)
    news_raw, errors = fetch_news()
    seen = set(state.get("seen", []))

    news = []
    dup = 0
    for item in news_raw:
        if item["uid"] in seen:
            dup += 1
            continue
        if item["time"] < cutoff:
            continue
        news.append(item)

    log(f"[去重] 原始 {len(news_raw)} 条 → 去重 {dup} 条 / 超窗过滤后 {len(news)} 条")

    hits = scan_news(news, idx)
    alerts = []
    for hit in hits:
        scored = score_hit(hit, quotes, cfg, idx)
        if scored and scored.get("level") in ("L1", "L2", "L3"):
            alerts.append(scored)

    alerts.sort(key=lambda x: ({"L1": 0, "L2": 1, "L3": 2}[x["level"]], -x["score"]))
    alerts = dedupe_events(alerts)
    alerts.sort(key=lambda x: ({"L1": 0, "L2": 1, "L3": 2}[x["level"]], -x["score"]))
    macro = macro_scan(idx, news, quotes,
                       {h["news"]["uid"] for h in hits})
    movers = price_movers(quotes, cfg)
    mover_attr = attribute_movers(movers, news, idx)

    push = [a for a in alerts if a["level"] in ("L1", "L2")]

    payload = {
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cfg": cfg,
        "quotes": quotes,
        "alerts": alerts,
        "macro": macro,
        "movers": movers,
        "mover_attr": mover_attr,
        "stats": {
            "news": len(news_raw), "sources": 4, "varieties": len(quotes),
            "new_alerts": len(alerts), "l1": sum(1 for a in alerts if a["level"] == "L1"),
            "l2": sum(1 for a in alerts if a["level"] == "L2"),
            "elapsed": round(time.time() - t0, 1), "errors": errors,
        },
    }

    now_epoch = time.time()
    for a in alerts:
        a.setdefault("ts", now_epoch)
    rolling = merge_rolling(alerts)

    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    report = render_report(payload)
    (ALERT_DIR / "latest.md").write_text(report, encoding="utf-8")
    (ALERT_DIR / "latest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    try:
        uid2var: dict[str, set[str]] = {}
        for h in hits:
            for vv in h["varieties"]:
                uid2var.setdefault(h["news"]["uid"], set()).add(vv["name"])
        news_view = [{"time": (n["time"].strftime("%H:%M")
                               if hasattr(n["time"], "strftime") else str(n["time"])),
                      "source": n["source"], "text": n["text"],
                      "variety": "、".join(sorted(uid2var.get(n["uid"], [])))} for n in news]
        inv = fetch_inventory(cfg)
        bas = fetch_basis(cfg)
        fund = cfg.get("fundamentals", {})
        (ALERT_DIR / "dashboard.html").write_text(
            render_dashboard(payload, news_view, inv, fund, bas, rolling), encoding="utf-8")
    except Exception as exc:                 # noqa: BLE001 看板失败不影响主流程
        log(f"[看板] 生成失败: {exc}")

    publish_dashboard(cfg)

    day_file = ALERT_DIR / f"{datetime.now():%Y-%m-%d}.jsonl"
    with day_file.open("a", encoding="utf-8") as fh:
        for a in alerts:
            body = {k: v for k, v in a.items() if k != "ts"}
            fh.write(json.dumps({"ts": payload["run_time"], **body}, ensure_ascii=False) + "\n")

    state["seen"] = list(seen | {n["uid"] for n in news})
    state["last_run"] = payload["run_time"]
    state["run_count"] = int(state.get("run_count", 0)) + 1
    save_state(state, cfg["thresholds"]["seen_cache_size"])

    if push:
        summary = f"【商品黑天鹅雷达 {payload['run_time']}】P0 {payload['stats']['l1']} / P1 {payload['stats']['l2']}\n"
        for a in push[:6]:
            summary += (f"· {a['variety']} {fmt_chg(a.get('chg'))} | "
                        f"{'/'.join(a['critical'] or a['major']) or '消息'} | {a['text'][:60]}\n")
        notify_webhook(cfg, summary)
        if cfg.get("desktop_notify", True):
            l1 = [a for a in push if a["level"] == "L1"]
            head = "🔴 P0 突发" if l1 else "🟠 P1 重要"
            body = "\n".join(
                f"{a['variety']}({a['code']}) {fmt_chg(a.get('chg'))}\n{a['text'][:90]}"
                for a in (l1 or push)[:3])
            notify_desktop(f"商品黑天鹅雷达 · {head}", body)

    log(f"[完成] P0={payload['stats']['l1']} P1={payload['stats']['l2']} "
        f"L3={len(alerts) - len(push)} 异动={len(movers)} 耗时={payload['stats']['elapsed']}s")
    return payload


def check(cfg: dict[str, Any]) -> int:
    """自检：配置 + 各数据源连通性。"""
    print("== 配置 ==")
    print(f"品种数：{len(cfg['varieties'])}")
    for code in ("rbm", "im", "pm", "SRM", "aum", "lcm"):
        names = [v["name"] for v in cfg["varieties"] if v["code"] == code]
        print(f"  {code}: {names or '缺失'}")
    print("\n== 数据源 ==")
    ok = True
    try:
        q = fetch_quotes(cfg)
        print(f"  行情 东方财富: {len(q)} 个合约 {'OK' if q else 'FAIL'}")
        ok &= bool(q)
    except Exception as exc:                 # noqa: BLE001
        print(f"  行情 FAIL {exc}")
        ok = False
    for name, fn in (("华尔街见闻", lambda: fetch_wscn("global-channel", 5)),
                     ("金十数据", fetch_jin10),
                     ("新浪7x24", lambda: fetch_sina(1))):
        try:
            print(f"  {name}: {len(fn())} 条 OK")
        except Exception as exc:             # noqa: BLE001
            print(f"  {name}: FAIL {exc}")
            ok = False
    print("\n自检结果：", "通过" if ok else "存在失败项")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="商品期货消息面黑天鹅雷达")
    parser.add_argument("--once", action="store_true", help="执行一次扫描")
    parser.add_argument("--daemon", action="store_true", help="常驻循环，每 30 分钟一次")
    parser.add_argument("--interval", type=int, default=1800, help="daemon 间隔秒数，默认 1800")
    parser.add_argument("--check", action="store_true", help="自检数据源与配置")
    parser.add_argument("--test-webhook", action="store_true", help="发一条测试消息验证推送")
    args = parser.parse_args()

    cfg = load_config()
    idx = build_index(cfg)

    if args.check:
        return check(cfg)

    if args.test_webhook:
        return test_webhook(cfg)

    if args.daemon:
        log(f"[启动] 常驻模式，间隔 {args.interval}s")
        first = not STATE_PATH.exists()
        while True:
            try:
                run_once(cfg, idx, first_run=first)
                first = False
            except Exception:                # noqa: BLE001 守护进程不能因单次失败退出
                log("[异常] 本轮扫描失败：\n" + traceback.format_exc())
            time.sleep(args.interval)

    if args.once or len(sys.argv) == 1:
        try:
            payload = run_once(cfg, idx, first_run=not STATE_PATH.exists())
        except Exception:                    # noqa: BLE001
            log("[异常] 扫描失败：\n" + traceback.format_exc())
            return 1
        # stdout 摘要，供 cron 邮件 / 终端查看
        print(report_head(payload))
        return 0

    parser.print_help()
    return 0


def report_head(payload: dict[str, Any]) -> str:
    st = payload["stats"]
    head = (f"\n{'=' * 58}\n商品黑天鹅雷达 · {payload['run_time']}\n"
            f"快讯 {st['news']} 条 / 品种 {st['varieties']} 个 / "
            f"P0 {st['l1']} · P1 {st['l2']} · P2 {st['new_alerts'] - st['l1'] - st['l2']}\n{'=' * 58}")
    for a in payload["alerts"]:
        if a["level"] in ("L1", "L2"):
            chg = f"{a['chg']:+.2f}%" if isinstance(a.get("chg"), (int, float)) else "-"
            head += f"\n[{a['level']}] {a['variety']}({a['code']}) {chg} | {a['time']} {a['source']}\n     {a['text'][:110]}"
    if not any(a["level"] in ("L1", "L2") for a in payload["alerts"]):
        head += "\n本次无黑天鹅级事件。"
    return head + f"\n\n完整报告：{ALERT_DIR / 'latest.md'}"


if __name__ == "__main__":
    sys.exit(main())
