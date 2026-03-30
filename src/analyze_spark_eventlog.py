"""
Spark Event Log 분석 스크립트

사용법:
  python3 analyze_spark_eventlog.py <eventlog_dir>

예시:
  python3 analyze_spark_eventlog.py eventlog_v2_application_1774495875024_0744_1
"""

import json
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# 유틸리티
# ---------------------------------------------------------------------------


def decompress_and_read(eventlog_dir: Path) -> list[str]:
    """zstd 압축된 이벤트 로그 파일들을 읽어 JSON 라인 리스트로 반환."""
    lines = []
    for zstd_file in sorted(eventlog_dir.glob("events_*.zstd")):
        result = subprocess.run(
            ["zstd", "-d", "-c", str(zstd_file)],
            capture_output=True,
        )
        lines.extend(result.stdout.decode("utf-8", errors="replace").splitlines())
    return lines


def parse_events(lines: list[str]) -> list[dict]:
    """JSON 라인을 파싱하여 이벤트 객체 리스트로 반환."""
    events = []
    for line in lines:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def ts_to_str(epoch_ms: int) -> str:
    """epoch 밀리초를 사람이 읽을 수 있는 문자열로 변환."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).strftime("%H:%M:%S")


def fmt_duration(ms: float) -> str:
    """밀리초를 읽기 쉬운 문자열로 변환."""
    if ms < 1000:
        return f"{ms:.0f}ms"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m = s / 60
    if m < 60:
        return f"{m:.1f}m"
    return f"{m / 60:.1f}h"


def fmt_bytes(b: int) -> str:
    """바이트를 읽기 쉬운 문자열로 변환."""
    if b < 1024:
        return f"{b}B"
    if b < 1024**2:
        return f"{b / 1024:.1f}KB"
    if b < 1024**3:
        return f"{b / 1024**2:.1f}MB"
    return f"{b / 1024**3:.1f}GB"


def print_section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def print_guide(lines: list[str]) -> None:
    """해석 가이드를 박스 안에 출력."""
    print()
    print(f"  ┌─ 해석 가이드 {'─' * 62}")
    for line in lines:
        print(f"  │ {line}")
    print(f"  └{'─' * 77}")


# ---------------------------------------------------------------------------
# 1. FAIR 스케줄러 Pool 분석
# ---------------------------------------------------------------------------


def analyze_pools(events: list[dict]) -> None:
    print_section("1. FAIR 스케줄러 Pool 배정")

    pool_jobs: dict[str, list[int]] = defaultdict(list)
    for e in events:
        if e.get("Event") != "SparkListenerJobStart":
            continue
        props = e.get("Properties", {})
        pool = props.get("spark.scheduler.pool", "default")
        pool_jobs[pool].append(e.get("Job ID", -1))

    if not pool_jobs:
        print("  SparkListenerJobStart 이벤트 없음")
        return

    total = sum(len(j) for j in pool_jobs.values())
    print(f"  {'Pool':<60} {'Jobs':>6}")
    print(f"  {'-' * 66}")
    for pool, jobs in sorted(pool_jobs.items(), key=lambda x: -len(x[1])):
        print(f"  {pool:<60} {len(jobs):>6}")

    print(f"\n  총 {total}개 Job, {len(pool_jobs)}개 Pool")
    if len(pool_jobs) == 1 and "default" in pool_jobs:
        print("  ⚠ 모든 Job이 default pool — FAIR 스케줄링 미작동")
    elif len(pool_jobs) > 1:
        print(f"  ✓ {len(pool_jobs)}개 토픽별 pool 배정 — FAIR 스케줄링 작동")

    print_guide(
        [
            "Spark FAIR 스케줄러는 pool 단위로 executor 리소스를 공정 분배한다.",
            "pool이 토픽별로 나뉘면, 무거운 토픽이 executor를 독점하지 않고",
            "가벼운 토픽도 리소스를 받을 수 있다.",
            "",
            "  ✓ 토픽별 pool 분리됨   → 정상. FAIR 스케줄링 작동 중",
            "  ⚠ 모두 default pool    → setLocalProperty가 foreachBatch까지 전파 안 됨",
            "",
            "Jobs 수가 토픽마다 다른 이유:",
            "  한 토픽의 foreachBatch 내부에서 여러 Spark Job이 실행된다.",
            "  (persist, schema 조회, MERGE INTO, DELETE, 통계 수집 등)",
            "  데이터가 많은 토픽일수록 배치 수가 많아 Job 수도 증가한다.",
        ]
    )


# ---------------------------------------------------------------------------
# 2. 토픽별 처리 시간 타임라인
# ---------------------------------------------------------------------------


def analyze_topic_timeline(events: list[dict]) -> None:
    print_section("2. 토픽별 처리 시간 타임라인")

    group_to_topic: dict[str, str] = {}
    job_start: dict[int, int] = {}
    job_end: dict[int, int] = {}
    job_group: dict[int, str] = {}

    for e in events:
        evt = e.get("Event")
        if evt == "SparkListenerJobStart":
            jid = int(e["Job ID"])
            props = e.get("Properties", {})
            gid = props.get("spark.jobGroup.id", "")
            desc = props.get("spark.job.description", "")
            topic = desc.split("\n")[0].strip() if desc else ""
            if gid and topic:
                group_to_topic[gid] = topic
            job_start[jid] = e.get("Submission Time", 0)
            job_group[jid] = gid
        elif evt == "SparkListenerJobEnd":
            jid = int(e["Job ID"])
            job_end[jid] = e.get("Completion Time", 0)

    # topic별 시작/종료 시간 집계
    topic_times: dict[str, dict] = {}
    for gid, topic in group_to_topic.items():
        related_jobs = [jid for jid, g in job_group.items() if g == gid]
        if not related_jobs:
            continue
        starts = [job_start[j] for j in related_jobs if j in job_start]
        ends = [job_end[j] for j in related_jobs if j in job_end]
        if starts and ends:
            t_start = min(starts)
            t_end = max(ends)
            topic_times[topic] = {
                "start": t_start,
                "end": t_end,
                "duration_ms": t_end - t_start,
                "job_count": len(related_jobs),
            }

    if not topic_times:
        print("  토픽 타임라인 데이터 없음")
        return

    app_start = min(t["start"] for t in topic_times.values())
    app_end = max(t["end"] for t in topic_times.values())
    sorted_topics = sorted(topic_times.items(), key=lambda x: x[1]["start"])

    # 통계 계산
    durations = sorted(t["duration_ms"] for t in topic_times.values())
    slowest = max(topic_times.items(), key=lambda x: x[1]["duration_ms"])

    print(f"  {'Topic':<55} {'Start':>8} {'End':>8} {'Duration':>10} {'Jobs':>5}")
    print(f"  {'-' * 88}")
    for topic, t in sorted_topics:
        print(
            f"  {topic:<55} "
            f"{ts_to_str(t['start']):>8} "
            f"{ts_to_str(t['end']):>8} "
            f"{fmt_duration(t['duration_ms']):>10} "
            f"{t['job_count']:>5}"
        )

    total_dur = app_end - app_start
    print(f"\n  앱 전체 실행 시간: {ts_to_str(app_start)} ~ {ts_to_str(app_end)} ({fmt_duration(total_dur)})")

    # 동시 실행 토픽 수 (1초 단위 샘플링)
    max_concurrent = 0
    max_concurrent_time = app_start
    for t_ms in range(app_start, app_end, 1000):
        concurrent = sum(1 for t in topic_times.values() if t["start"] <= t_ms <= t["end"])
        if concurrent > max_concurrent:
            max_concurrent = concurrent
            max_concurrent_time = t_ms

    print(f"  최대 동시 실행 토픽 수: {max_concurrent} (at {ts_to_str(max_concurrent_time)})")

    # Gantt 차트
    bar_width = 60
    print(f"\n  타임라인 (각 █ = {fmt_duration((app_end - app_start) / bar_width)})")
    print(f"  {'Topic':<40} |{'─' * bar_width}|")
    for topic, t in sorted_topics:
        short_name = topic.split(".")[-1][:38]
        start_pos = int((t["start"] - app_start) / max(1, app_end - app_start) * bar_width)
        end_pos = int((t["end"] - app_start) / max(1, app_end - app_start) * bar_width)
        end_pos = max(end_pos, start_pos + 1)
        bar = " " * start_pos + "█" * (end_pos - start_pos) + " " * (bar_width - end_pos)
        print(f"  {short_name:<40} |{bar}|")

    print_guide(
        [
            "Gantt 차트는 각 토픽이 언제 시작/종료되었는지 시각화한다.",
            "세마포어(concurrency)가 정상 작동하면 동시 실행 토픽 수가 concurrency와 일치한다.",
            "",
            f"  최대 동시 실행: {max_concurrent}개  → concurrency 설정과 비교하여 정상 여부 판단",
            f"  가장 느린 토픽: {slowest[0].split('.')[-1]} ({fmt_duration(slowest[1]['duration_ms'])})",
            f"  가장 빠른 토픽: {fmt_duration(durations[0])}",
            f"  Duration 편차: {fmt_duration(durations[0])} ~ {fmt_duration(durations[-1])}",
            "",
            "확인 포인트:",
            "  - 앱 초반에 긴 막대가 몰리면 → 초기 토픽이 무거워서 나머지가 대기 중",
            "  - 막대가 겹치는 구간이 많으면 → concurrency만큼 동시 실행되고 있음 (정상)",
            "  - 막대 사이 빈 공간이 크면  → 토픽 간 전환 오버헤드 또는 세마포어 대기",
        ]
    )


# ---------------------------------------------------------------------------
# 3. SQL 쿼리 성능
# ---------------------------------------------------------------------------


def analyze_sql_performance(events: list[dict]) -> None:
    print_section("3. SQL 쿼리 성능")

    sql_start: dict[int, dict] = {}
    sql_end: dict[int, int] = {}

    for e in events:
        evt = e.get("Event")
        if evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
            eid = int(e["executionId"])
            sql_start[eid] = {
                "description": e.get("description", ""),
                "time": e.get("time", 0),
            }
        elif evt == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
            eid = int(e["executionId"])
            sql_end[eid] = e.get("time", 0)

    if not sql_start:
        print("  SQL 실행 이벤트 없음")
        return

    sql_types: dict[str, list[dict]] = defaultdict(list)
    for eid, info in sql_start.items():
        if eid not in sql_end:
            continue
        dur_ms = sql_end[eid] - info["time"]
        desc = info["description"]

        desc_lower = desc.lower()
        if "merge into" in desc_lower:
            sql_type = "MERGE INTO"
        elif "delete from" in desc_lower:
            sql_type = "DELETE"
        elif "insert into" in desc_lower:
            sql_type = "INSERT INTO"
        elif "create table" in desc_lower or "create database" in desc_lower:
            sql_type = "DDL"
        elif "select" in desc_lower:
            sql_type = "SELECT"
        else:
            sql_type = "OTHER"

        sql_types[sql_type].append(
            {
                "id": eid,
                "duration_ms": dur_ms,
                "description": desc[:120],
            }
        )

    print(f"  {'SQL Type':<15} {'Count':>6} {'Avg':>10} {'P50':>10} {'P95':>10} {'Max':>10} {'Total':>10}")
    print(f"  {'-' * 75}")
    for sql_type, queries in sorted(sql_types.items(), key=lambda x: -sum(q["duration_ms"] for q in x[1])):
        durations = sorted(q["duration_ms"] for q in queries)
        cnt = len(durations)
        total = sum(durations)
        avg = total / cnt
        p50 = durations[cnt // 2]
        p95 = durations[int(cnt * 0.95)]
        mx = durations[-1]
        print(
            f"  {sql_type:<15} {cnt:>6} "
            f"{fmt_duration(avg):>10} {fmt_duration(p50):>10} "
            f"{fmt_duration(p95):>10} {fmt_duration(mx):>10} "
            f"{fmt_duration(total):>10}"
        )

    all_queries = []
    for queries in sql_types.values():
        all_queries.extend(queries)
    all_queries.sort(key=lambda x: -x["duration_ms"])

    print("\n  느린 SQL Top 10:")
    print(f"  {'ID':>5} {'Duration':>10}  {'Description'}")
    print(f"  {'-' * 75}")
    for q in all_queries[:10]:
        print(f"  {q['id']:>5} {fmt_duration(q['duration_ms']):>10}  {q['description'][:60]}")

    # OTHER만 있는 경우 설명 추가
    only_other = len(sql_types) == 1 and "OTHER" in sql_types

    guide = [
        "Structured Streaming의 foreachBatch 안에서 실행되는 SQL의 성능이다.",
        "MERGE INTO(upsert)와 DELETE가 CDC 파이프라인의 핵심 작업.",
        "",
        "  P50 (중앙값) : 쿼리의 절반은 이 시간 안에 완료",
        "  P95          : 95%의 쿼리가 이 시간 안에 완료. P50과 차이가 크면 일부 쿼리가 느린 것",
        "  Max           : 가장 느린 단일 쿼리. 병목 후보",
        "  Total         : SQL 유형별 누적 시간. 어디에 시간이 가장 많이 쓰이는지 파악",
        "",
        "느린 SQL Top 10에서:",
        "  - 같은 토픽이 반복되면 → 해당 테이블의 데이터량 또는 스키마 문제",
        "  - Duration이 분 단위면 → Iceberg MERGE INTO가 대용량 데이터 처리 중",
    ]
    if only_other:
        guide.extend(
            [
                "",
                "※ 모든 SQL이 OTHER로 분류됨:",
                "  Structured Streaming에서는 SQL description이 쿼리문이 아닌",
                "  스트리밍 쿼리 이름(토픽명)으로 기록된다.",
                "  MERGE INTO/DELETE별 성능은 Stage 레벨에서 간접적으로 판단해야 한다.",
            ]
        )

    print_guide(guide)


# ---------------------------------------------------------------------------
# 4. Task Skew 탐지
# ---------------------------------------------------------------------------


def analyze_task_skew(events: list[dict]) -> None:
    print_section("4. Task Skew 탐지")

    stage_tasks: dict[int, list[dict]] = defaultdict(list)

    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd":
            continue
        task_info = e.get("Task Info", {})
        task_metrics = e.get("Task Metrics", {})
        stage_id = e.get("Stage ID", -1)

        if task_info.get("Failed", False):
            continue

        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
        shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
        shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
        input_metrics = task_metrics.get("Input Metrics", {})

        stage_tasks[stage_id].append(
            {
                "duration_ms": duration,
                "gc_time_ms": task_metrics.get("JVM GC Time", 0),
                "input_bytes": input_metrics.get("Bytes Read", 0),
                "shuffle_read_bytes": shuffle_read.get("Remote Bytes Read", 0)
                + shuffle_read.get("Local Bytes Read", 0),
                "shuffle_write_bytes": shuffle_write.get("Shuffle Bytes Written", 0),
                "executor_id": task_info.get("Executor ID", ""),
            }
        )

    if not stage_tasks:
        print("  Task 이벤트 없음")
        return

    # Skew가 심한 Stage 탐지 (max/median > 3x 이상)
    skewed_stages = []
    for stage_id, tasks in stage_tasks.items():
        if len(tasks) < 2:
            continue
        durations = sorted(t["duration_ms"] for t in tasks)
        median = durations[len(durations) // 2]
        mx = durations[-1]
        if median > 0 and mx / median > 3:
            skewed_stages.append(
                {
                    "stage_id": stage_id,
                    "task_count": len(tasks),
                    "min_ms": durations[0],
                    "median_ms": median,
                    "max_ms": mx,
                    "skew_ratio": mx / median,
                }
            )

    skewed_stages.sort(key=lambda x: -x["skew_ratio"])

    if skewed_stages:
        print(f"  Skew 감지 (max/median > 3x): {len(skewed_stages)}개 Stage")
        print(f"\n  {'Stage':>6} {'Tasks':>6} {'Min':>10} {'Median':>10} {'Max':>10} {'Skew':>6}")
        print(f"  {'-' * 52}")
        for s in skewed_stages[:15]:
            print(
                f"  {s['stage_id']:>6} {s['task_count']:>6} "
                f"{fmt_duration(s['min_ms']):>10} {fmt_duration(s['median_ms']):>10} "
                f"{fmt_duration(s['max_ms']):>10} {s['skew_ratio']:>5.1f}x"
            )
    else:
        print("  ✓ Skew 없음 (모든 Stage에서 max/median < 3x)")

    # 전체 Task 통계
    all_durations = [t["duration_ms"] for tasks in stage_tasks.values() for t in tasks]
    all_gc = [t["gc_time_ms"] for tasks in stage_tasks.values() for t in tasks]
    all_durations.sort()
    total_tasks = len(all_durations)

    print(f"\n  전체 Task 통계 ({total_tasks}개):")
    print(
        f"    Duration  — P50: {fmt_duration(all_durations[total_tasks // 2])}, "
        f"P95: {fmt_duration(all_durations[int(total_tasks * 0.95)])}, "
        f"Max: {fmt_duration(all_durations[-1])}"
    )
    gc_total = sum(all_gc)
    dur_total = sum(all_durations)
    gc_pct = gc_total / max(1, dur_total) * 100
    print(f"    GC 비율   — {gc_pct:.1f}% (GC 총합: {fmt_duration(gc_total)}, 실행 총합: {fmt_duration(dur_total)})")

    # Skew 심각도 판정
    serious_skew = [s for s in skewed_stages if s["max_ms"] > 10000]  # max > 10초

    guide = [
        "Task Skew: 같은 Stage 내 태스크 간 실행시간 편차.",
        "한 태스크가 다른 태스크보다 훨씬 오래 걸리면 전체 Stage가 그 태스크를 기다린다.",
        "",
        "  Skew 비율 = Max / Median",
        "    < 3x   : 정상 범위",
        "    3~10x  : 경미한 skew. Max 절대값이 작으면 (< 1초) 무시해도 됨",
        "    > 10x  : 심각한 skew. Max 절대값도 크면 데이터 파티셔닝 점검 필요",
        "",
    ]

    if serious_skew:
        guide.append(f"  ⚠ Max > 10초인 Skew Stage: {len(serious_skew)}개 — 파티셔닝 점검 권장")
    elif skewed_stages:
        guide.append("  ✓ Skew가 감지되었으나 Max 절대값이 작아 실질적 영향 미미")
    else:
        guide.append("  ✓ Skew 없음")

    guide.extend(
        [
            "",
            "GC 비율:",
            "  < 5%  : 정상. 메모리 여유 충분",
            "  5~10% : 주의. executor 메모리 증설 고려",
            "  > 10% : 심각. GC 오버헤드로 성능 저하 발생 중",
            f"  현재: {gc_pct:.1f}%",
        ]
    )

    print_guide(guide)


# ---------------------------------------------------------------------------
# 5. Executor 활용률
# ---------------------------------------------------------------------------


def analyze_executors(events: list[dict]) -> None:
    print_section("5. Executor 활용률")

    executors: dict[str, dict] = {}
    for e in events:
        evt = e.get("Event")
        if evt == "SparkListenerExecutorAdded":
            eid = e.get("Executor ID", "")
            info = e.get("Executor Info", {})
            executors[eid] = {
                "host": info.get("Host", ""),
                "cores": info.get("Total Cores", 0),
                "added_time": e.get("Timestamp", 0),
            }
        elif evt == "SparkListenerBlockManagerAdded":
            eid = e.get("Block Manager ID", {}).get("Executor ID", "")
            if eid in executors:
                executors[eid]["memory"] = e.get("Maximum Memory", 0)

    if not executors:
        print("  Executor 이벤트 없음")
        return

    print(f"  {'Executor':>10} {'Host':<30} {'Cores':>6} {'Memory':>10} {'Added':>10}")
    print(f"  {'-' * 70}")
    for eid, info in sorted(executors.items()):
        mem = fmt_bytes(info.get("memory", 0)) if "memory" in info else "N/A"
        added = ts_to_str(info["added_time"]) if info["added_time"] else "N/A"
        print(f"  {eid:>10} {info['host']:<30} {info['cores']:>6} {mem:>10} {added:>10}")

    # Executor별 task 처리량
    executor_tasks: dict[str, list[int]] = defaultdict(list)
    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd":
            continue
        task_info = e.get("Task Info", {})
        eid = task_info.get("Executor ID", "")
        dur = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
        executor_tasks[eid].append(dur)

    if not executor_tasks:
        return

    app_start = min(info["added_time"] for info in executors.values() if info["added_time"])
    app_end_events = [e for e in events if e.get("Event") == "SparkListenerApplicationEnd"]
    app_end = app_end_events[0].get("Timestamp", 0) if app_end_events else 0
    app_duration = max(1, app_end - app_start)

    print(f"\n  {'Executor':>10} {'Tasks':>7} {'Avg Duration':>12} {'Total Busy':>12} {'활용률':>8}")
    print(f"  {'-' * 55}")

    utilizations = []
    for eid in sorted(executor_tasks.keys()):
        tasks = executor_tasks[eid]
        total_busy = sum(tasks)
        cores = executors.get(eid, {}).get("cores", 1)
        utilization = total_busy / max(1, app_duration * cores) * 100
        avg_dur = total_busy / len(tasks)
        utilizations.append(utilization)
        print(
            f"  {eid:>10} {len(tasks):>7} "
            f"{fmt_duration(avg_dur):>12} {fmt_duration(total_busy):>12} "
            f"{utilization:>7.1f}%"
        )

    avg_util = sum(utilizations) / len(utilizations) if utilizations else 0
    max_util = max(utilizations) if utilizations else 0
    min_util = min(utilizations) if utilizations else 0
    total_cores = sum(info.get("cores", 0) for info in executors.values())
    total_memory = sum(info.get("memory", 0) for info in executors.values())

    print_guide(
        [
            "각 executor가 앱 실행 시간 동안 얼마나 바쁘게 일했는지 보여준다.",
            "",
            "칼럼 설명:",
            "  Tasks       : executor에 할당된 총 task 수",
            "  Avg Duration: task 하나의 평균 실행 시간",
            "  Total Busy  : executor가 실제로 task를 실행한 총 시간",
            "  활용률       : Total Busy / (앱 실행 시간 × core 수) × 100",
            "",
            "활용률 해석:",
            "  > 80%  : executor를 충분히 활용 중. 더 줄이면 성능 저하 위험",
            "  50~80% : 적정 수준. 약간의 여유 있음",
            "  30~50% : executor가 놀고 있는 시간이 많음. 인스턴스 수 축소 가능",
            "  < 30%  : 과잉 할당. executor 줄이거나 concurrency 올려서 활용도 개선",
            "",
            f"  현재 평균 활용률: {avg_util:.1f}% (범위: {min_util:.1f}% ~ {max_util:.1f}%)",
            f"  Executor 간 편차: {max_util - min_util:.1f}%p",
            "    편차 < 10%p → executor 간 부하 균등 (정상)",
            "    편차 > 20%p → 특정 executor에 부하 집중 (FAIR 스케줄러 또는 데이터 locality 점검)",
            "",
            "리소스 요약:",
            f"  Executor {len(executors)}대 × {executors[next(iter(executors))].get('cores', '?')} cores = 총 {total_cores} cores",
            f"  총 메모리: {fmt_bytes(total_memory)} (executor 메모리 합계, JVM heap)",
            f"  앱 실행 시간: {fmt_duration(app_duration)}",
        ]
    )


# ---------------------------------------------------------------------------
# 6. 메모리 분석
# ---------------------------------------------------------------------------


def analyze_memory(events: list[dict]) -> None:
    print_section("6. 메모리 분석")

    # Executor별 JVM Heap (BlockManager에서 추출)
    executor_heap: dict[str, int] = {}
    for e in events:
        if e.get("Event") == "SparkListenerBlockManagerAdded":
            eid = e.get("Block Manager ID", {}).get("Executor ID", "")
            if eid != "driver":
                executor_heap[eid] = e.get("Maximum Memory", 0)

    # Task 레벨 메모리 지표 수집
    executor_peak: dict[str, int] = defaultdict(int)
    executor_spill_disk: dict[str, int] = defaultdict(int)
    executor_spill_memory: dict[str, int] = defaultdict(int)
    executor_input_bytes: dict[str, int] = defaultdict(int)
    executor_shuffle_read: dict[str, int] = defaultdict(int)
    executor_shuffle_write: dict[str, int] = defaultdict(int)

    total_spill_disk = 0
    total_spill_memory = 0

    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd":
            continue
        task_info = e.get("Task Info", {})
        metrics = e.get("Task Metrics", {})
        eid = task_info.get("Executor ID", "")

        if task_info.get("Failed", False):
            continue

        peak = metrics.get("Peak Execution Memory", 0)
        if peak > executor_peak[eid]:
            executor_peak[eid] = peak

        disk_spill = metrics.get("Disk Bytes Spilled", 0)
        mem_spill = metrics.get("Memory Bytes Spilled", 0)
        executor_spill_disk[eid] += disk_spill
        executor_spill_memory[eid] += mem_spill
        total_spill_disk += disk_spill
        total_spill_memory += mem_spill

        input_m = metrics.get("Input Metrics", {})
        shuffle_r = metrics.get("Shuffle Read Metrics", {})
        shuffle_w = metrics.get("Shuffle Write Metrics", {})
        executor_input_bytes[eid] += input_m.get("Bytes Read", 0)
        executor_shuffle_read[eid] += shuffle_r.get("Remote Bytes Read", 0) + shuffle_r.get("Local Bytes Read", 0)
        executor_shuffle_write[eid] += shuffle_w.get("Shuffle Bytes Written", 0)

    if not executor_peak:
        print("  Task 이벤트 없음")
        return

    # Executor별 메모리 현황
    print(f"  {'Executor':>10} {'JVM Heap':>10} {'Peak Exec':>10} {'사용률':>7} {'Disk Spill':>11} {'Mem Spill':>11}")
    print(f"  {'-' * 65}")
    peak_ratios = []
    for eid in sorted(executor_peak.keys()):
        heap = executor_heap.get(eid, 0)
        peak = executor_peak[eid]
        ratio = peak / max(1, heap) * 100
        peak_ratios.append(ratio)
        print(
            f"  {eid:>10} {fmt_bytes(heap):>10} {fmt_bytes(peak):>10} "
            f"{ratio:>6.0f}% {fmt_bytes(executor_spill_disk[eid]):>11} "
            f"{fmt_bytes(executor_spill_memory[eid]):>11}"
        )

    avg_peak_ratio = sum(peak_ratios) / len(peak_ratios) if peak_ratios else 0

    # Spill 요약
    print("\n  Spill 요약:")
    print(f"    Memory Spill 총합: {fmt_bytes(total_spill_memory)}")
    print(f"    Disk Spill 총합:   {fmt_bytes(total_spill_disk)}")

    if total_spill_disk > 0:
        print("    ⚠ Disk Spill 발생 — shuffle/집계 데이터가 메모리에 안 들어가 디스크 I/O 발생")
    else:
        print("    ✓ Disk Spill 없음")

    # I/O 요약
    total_input = sum(executor_input_bytes.values())
    total_shuf_r = sum(executor_shuffle_read.values())
    total_shuf_w = sum(executor_shuffle_write.values())

    print("\n  I/O 요약:")
    print(f"    Input (Kafka 읽기): {fmt_bytes(total_input)}")
    print(f"    Shuffle Write:      {fmt_bytes(total_shuf_w)}")
    print(f"    Shuffle Read:       {fmt_bytes(total_shuf_r)}")

    guide = [
        "Executor의 메모리 사용 현황과 디스크 spill 여부를 보여준다.",
        "",
        "■ Spark Executor 메모리 구조:",
        "  executor.memory (YAML 설정값, 예: 4GB)",
        "    └─ JVM Heap = executor.memory × spark.memory.fraction (기본 0.6)",
        "         ├─ Storage Memory (persist 등): heap × storageFraction (기본 0.2)",
        "         └─ Execution Memory (shuffle, 집계): 나머지",
        "    └─ User Memory = executor.memory × 0.4 (UDF, 데이터 구조체 등)",
        "",
        "■ 지표 계산 방법:",
        "  JVM Heap  : SparkListenerBlockManagerAdded → Maximum Memory",
        "              Spark이 관리하는 통합 메모리 (Storage + Execution)",
        "  Peak Exec : SparkListenerTaskEnd → Peak Execution Memory",
        "              단일 Task가 shuffle/집계에 사용한 최대 메모리",
        "  사용률     : Peak Exec / JVM Heap × 100",
        "  Disk Spill : SparkListenerTaskEnd → Disk Bytes Spilled",
        "              Execution Memory가 부족하여 디스크에 쓴 데이터량",
        "  Mem Spill  : SparkListenerTaskEnd → Memory Bytes Spilled",
        "              디스크로 내보내기 전 메모리에서 직렬화한 중간 데이터량",
        "",
        "■ 메모리 적정치 판단 기준:",
        "",
        "  지표              상태          의미                     조치",
        "  ─────────────────────────────────────────────────────────────────",
        "  Disk Spill > 0    메모리 부족   shuffle 데이터가 디스크   executor.memory",
        "                    확정          에 넘쳐 I/O 병목 발생    1.5~2배 증설",
        "  ─────────────────────────────────────────────────────────────────",
        "  사용률 > 80%      위험 구간     조금만 데이터 증가해도   executor.memory",
        "                                  spill 발생 가능          20~50% 증설",
        "  ─────────────────────────────────────────────────────────────────",
        "  사용률 50~80%     적정          현재 상태 유지           없음",
        "  ─────────────────────────────────────────────────────────────────",
        "  사용률 < 30%      과잉 할당     YARN 리소스 낭비         executor.memory",
        "                                                          축소 가능",
        "  ─────────────────────────────────────────────────────────────────",
        "",
        f"  현재 평균 사용률: {avg_peak_ratio:.0f}%",
    ]

    if total_spill_disk > 0:
        # Spill 해소에 필요한 메모리 추정
        max_spill_executor = max(executor_spill_disk.values())
        guide.extend(
            [
                "",
                f"  ⚠ Disk Spill {fmt_bytes(total_spill_disk)} 발생:",
                f"    가장 많이 spill한 executor: {fmt_bytes(max_spill_executor)}",
                "    조치: executor.memory를 1.5~2배로 증설하여 spill 해소",
                "    spill이 해소되면 디스크 I/O가 제거되어 처리 시간이 크게 단축된다.",
            ]
        )
    elif avg_peak_ratio < 30:
        guide.extend(
            [
                "",
                "  ✓ 메모리 여유 충분. executor.memory 축소 가능 (YARN 리소스 절약)",
            ]
        )
    else:
        guide.extend(
            [
                "",
                "  ✓ 메모리 사용 적정 수준",
            ]
        )

    guide.extend(
        [
            "",
            "Shuffle 해석:",
            "  Shuffle은 MERGE INTO 등 join/집계 시 executor 간 데이터 교환이다.",
            "  Shuffle Write ≈ Shuffle Read이면 정상.",
            "  Shuffle이 Input보다 훨씬 크면 비효율적 join이 발생 중일 수 있다.",
            f"  현재: Input {fmt_bytes(total_input)}, Shuffle {fmt_bytes(total_shuf_w)}",
        ]
    )

    print_guide(guide)


# ---------------------------------------------------------------------------
# 7. CPU 분석
# ---------------------------------------------------------------------------


def analyze_cpu(events: list[dict]) -> None:
    print_section("7. CPU 분석")

    # Executor 정보
    executors: dict[str, dict] = {}
    for e in events:
        if e.get("Event") == "SparkListenerExecutorAdded":
            eid = e.get("Executor ID", "")
            info = e.get("Executor Info", {})
            executors[eid] = {
                "host": info.get("Host", ""),
                "cores": info.get("Total Cores", 0),
            }

    # Task별 CPU 시간 분해
    executor_cpu: dict[str, dict] = defaultdict(
        lambda: {
            "executor_run_time": 0,
            "executor_cpu_time": 0,  # 나노초
            "jvm_gc_time": 0,
            "ser_time": 0,
            "deser_time": 0,
            "shuffle_fetch_wait": 0,
            "task_count": 0,
            "total_duration": 0,
        }
    )

    for e in events:
        if e.get("Event") != "SparkListenerTaskEnd":
            continue
        task_info = e.get("Task Info", {})
        metrics = e.get("Task Metrics", {})
        eid = task_info.get("Executor ID", "")

        if task_info.get("Failed", False):
            continue

        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
        shuffle_r = metrics.get("Shuffle Read Metrics", {})

        c = executor_cpu[eid]
        c["executor_run_time"] += metrics.get("Executor Run Time", 0)
        c["executor_cpu_time"] += metrics.get("Executor CPU Time", 0)
        c["jvm_gc_time"] += metrics.get("JVM GC Time", 0)
        c["ser_time"] += metrics.get("Result Serialization Time", 0)
        c["deser_time"] += metrics.get("Executor Deserialize Time", 0)
        c["shuffle_fetch_wait"] += shuffle_r.get("Fetch Wait Time", 0)
        c["task_count"] += 1
        c["total_duration"] += duration

    if not executor_cpu:
        print("  Task 이벤트 없음")
        return

    # Executor별 CPU 시간 분해
    print(
        f"  {'Executor':>10} {'Tasks':>6} {'Run Time':>10} {'CPU Time':>10} {'GC':>10} {'Shuffle Wait':>12} {'기타':>10}"
    )
    print(f"  {'-' * 75}")

    total_run = 0
    total_cpu = 0
    total_gc = 0
    total_shuffle_wait = 0
    total_ser = 0
    total_deser = 0
    total_duration = 0

    for eid in sorted(executor_cpu.keys()):
        c = executor_cpu[eid]
        cpu_ms = c["executor_cpu_time"] / 1_000_000  # 나노초 → 밀리초
        other = max(
            0, c["total_duration"] - c["executor_run_time"] - c["jvm_gc_time"] - c["ser_time"] - c["deser_time"]
        )
        print(
            f"  {eid:>10} {c['task_count']:>6} "
            f"{fmt_duration(c['executor_run_time']):>10} "
            f"{fmt_duration(cpu_ms):>10} "
            f"{fmt_duration(c['jvm_gc_time']):>10} "
            f"{fmt_duration(c['shuffle_fetch_wait']):>12} "
            f"{fmt_duration(other):>10}"
        )

        total_run += c["executor_run_time"]
        total_cpu += cpu_ms
        total_gc += c["jvm_gc_time"]
        total_shuffle_wait += c["shuffle_fetch_wait"]
        total_ser += c["ser_time"]
        total_deser += c["deser_time"]
        total_duration += c["total_duration"]

    # 시간 분해 요약
    total_other = max(0, total_duration - total_run - total_gc - total_ser - total_deser)
    _ = total_run - total_shuffle_wait  # 순수 연산 시간 (effective_compute, 향후 활용)

    print("\n  시간 분해 요약 (전체 executor 합산):")
    print(f"    총 Task 실행 시간: {fmt_duration(total_duration)}")
    print(
        f"    ├─ Executor Run Time:    {fmt_duration(total_run)} ({total_run / max(1, total_duration) * 100:.1f}%) — 실제 연산"
    )
    print(f"    │  ├─ CPU Time:          {fmt_duration(total_cpu)} — 실제 CPU 사용")
    print(f"    │  └─ Shuffle Wait:      {fmt_duration(total_shuffle_wait)} — 다른 executor 데이터 대기")
    print(
        f"    ├─ JVM GC Time:          {fmt_duration(total_gc)} ({total_gc / max(1, total_duration) * 100:.1f}%) — 가비지 컬렉션"
    )
    print(
        f"    ├─ Serialization:        {fmt_duration(total_ser + total_deser)} ({(total_ser + total_deser) / max(1, total_duration) * 100:.1f}%) — 직렬화/역직렬화"
    )
    print(
        f"    └─ 기타 (스케줄링 등):    {fmt_duration(total_other)} ({total_other / max(1, total_duration) * 100:.1f}%)"
    )

    # CPU 효율
    cpu_efficiency = total_cpu / max(1, total_run) * 100
    shuffle_wait_pct = total_shuffle_wait / max(1, total_run) * 100

    guide = [
        "Task 실행 시간이 어디에 쓰이는지 분해하여 병목 원인을 파악한다.",
        "",
        "■ 지표 계산 방법:",
        "  Run Time     : SparkListenerTaskEnd → Executor Run Time",
        "                 executor에서 Task 코드를 실행한 시간 (CPU + I/O + 대기 포함)",
        "  CPU Time     : SparkListenerTaskEnd → Executor CPU Time (나노초 단위)",
        "                 실제 CPU 사이클을 소비한 시간. Run Time의 부분집합",
        "                 Run Time - CPU Time = I/O 대기, shuffle 대기 등",
        "  GC           : SparkListenerTaskEnd → JVM GC Time",
        "                 가비지 컬렉션에 소비된 시간. Run Time과 겹칠 수 있음",
        "  Shuffle Wait : SparkListenerTaskEnd → Shuffle Read Metrics → Fetch Wait Time",
        "                 다른 executor에서 shuffle 데이터를 네트워크로 받기 위해 대기한 시간",
        "  Serialization: Executor Deserialize Time + Result Serialization Time",
        "                 Task 입력 역직렬화 + 결과 직렬화 오버헤드",
        "",
        "■ 시간 분해 다이어그램:",
        "  총 Task 실행 시간",
        "  ├─ Run Time (실제 연산) ─── 대부분의 시간이 여기에 사용됨",
        "  │    ├─ CPU Time ────────── 순수 CPU 연산 (비율이 높으면 CPU 바운드)",
        "  │    ├─ Shuffle Wait ────── 네트워크 대기 (비율이 높으면 네트워크 바운드)",
        "  │    └─ Disk I/O 대기 ───── spill 읽기/쓰기 (Disk Spill이 있으면 여기가 큼)",
        "  ├─ GC Time ─────────────── 메모리 정리 (메모리 부족 시 증가)",
        "  ├─ Serialization ────────── 직렬화/역직렬화 오버헤드",
        "  └─ 기타 ─────────────────── Task 스케줄링, 큐 대기 등",
        "",
        "■ 핵심 지표 판단 기준:",
        f"  CPU 효율 (CPU Time / Run Time): {cpu_efficiency:.0f}%",
    ]

    guide.extend(
        [
            "",
            "  지표                상태        의미                     조치",
            "  ──────────────────────────────────────────────────────────────────",
            "  CPU 효율 > 80%     CPU 바운드  CPU 연산이 대부분         executor.cores 증가",
            "                                                          또는 instances 증가",
            "  ──────────────────────────────────────────────────────────────────",
            "  CPU 효율 50~80%    적정        CPU와 I/O가 균형          현재 유지",
            "  ──────────────────────────────────────────────────────────────────",
            "  CPU 효율 < 50%     I/O 바운드  디스크/네트워크 대기가    Disk Spill 해소,",
            "                                 CPU보다 많음              데이터 locality 개선",
            "  ──────────────────────────────────────────────────────────────────",
        ]
    )

    if cpu_efficiency > 80:
        guide.append(f"  현재: {cpu_efficiency:.0f}% → CPU 바운드")
    elif cpu_efficiency > 50:
        guide.append(f"  현재: {cpu_efficiency:.0f}% → 적정")
    else:
        guide.append(f"  현재: {cpu_efficiency:.0f}% → I/O 바운드")

    guide.extend(
        [
            "",
            f"  Shuffle Wait 비율 (Wait / Run): {shuffle_wait_pct:.1f}%",
            "",
            "  지표                상태        의미                     조치",
            "  ──────────────────────────────────────────────────────────────────",
            "  Shuffle Wait > 20% 높음        executor 간 데이터       shuffle partition 조정,",
            "                                 교환이 병목              broadcast join 검토",
            "  ──────────────────────────────────────────────────────────────────",
            "  Shuffle Wait 5~20% 적정        일반적인 수준             현재 유지",
            "  ──────────────────────────────────────────────────────────────────",
            "  Shuffle Wait < 5%  낮음        shuffle이 병목 아님       현재 유지",
            "  ──────────────────────────────────────────────────────────────────",
        ]
    )

    if shuffle_wait_pct > 20:
        guide.append(f"  현재: {shuffle_wait_pct:.1f}% → 높음")
    elif shuffle_wait_pct > 5:
        guide.append(f"  현재: {shuffle_wait_pct:.1f}% → 적정")
    else:
        guide.append(f"  현재: {shuffle_wait_pct:.1f}% → 낮음")

    gc_pct = total_gc / max(1, total_duration) * 100
    guide.extend(
        [
            "",
            f"  GC 비율: {gc_pct:.1f}%",
            "    < 5%: 정상  |  5~10%: executor.memory 증설 고려  |  > 10%: 증설 필수",
            "",
            "■ 종합 판단 플로우:",
            "  1. CPU 효율이 낮은가? (<50%)",
            "     → Yes: Disk Spill 확인 → spill이 있으면 메모리 증설이 우선",
            "     → No: CPU가 병목이므로 cores/instances 증가",
            "  2. Shuffle Wait가 높은가? (>20%)",
            "     → Yes: shuffle partition 수 조정, 데이터 크기 축소(스트리밍 전환 등)",
            "  3. GC가 높은가? (>5%)",
            "     → Yes: executor.memory 증설",
        ]
    )

    print_guide(guide)


# ---------------------------------------------------------------------------
# 8. Streaming 배치 진행 상황
# ---------------------------------------------------------------------------


def analyze_streaming(events: list[dict]) -> None:
    print_section("8. Streaming 배치 진행 상황")

    queries: dict[str, dict] = {}
    progress_events: list[dict] = []

    for e in events:
        evt = e.get("Event", "")
        if "QueryStartedEvent" in evt:
            rid = e.get("runId", "")
            queries[rid] = {
                "name": e.get("name", ""),
                "id": e.get("id", ""),
                "start": e.get("timestamp", 0),
            }
        elif "QueryTerminatedEvent" in evt:
            rid = e.get("runId", "")
            if rid in queries:
                queries[rid]["exception"] = e.get("exception")
        elif "QueryProgressEvent" in evt:
            progress_events.append(e.get("progress", {}))

    if not queries:
        print("  Streaming 쿼리 없음")
        return

    succeeded = sum(1 for q in queries.values() if q.get("exception") is None)
    failed = sum(1 for q in queries.values() if q.get("exception") is not None)
    print(f"  총 {len(queries)}개 스트리밍 쿼리 (성공: {succeeded}, 실패: {failed})")

    if failed > 0:
        print("\n  실패한 쿼리:")
        for rid, q in queries.items():
            if q.get("exception"):
                print(f"    {q['name']}: {q['exception'][:100]}")

    if progress_events:
        print(f"\n  배치 Progress ({len(progress_events)}건):")
        print(f"  {'Query Name':<55} {'Batch':>6} {'Rows':>10} {'In/s':>10} {'Out/s':>10}")
        print(f"  {'-' * 95}")
        for p in progress_events:
            name = p.get("name", "")[:53]
            batch = p.get("batchId", "")
            rows = p.get("numInputRows", 0)
            in_rate = p.get("inputRowsPerSecond", 0)
            out_rate = p.get("processedRowsPerSecond", 0)
            print(f"  {name:<55} {batch:>6} {rows:>10} {in_rate:>9.1f} {out_rate:>9.1f}")

    # availableNow 판정: 배치 수가 적고 쿼리가 종료됨
    avg_batches = len(progress_events) / max(1, len(queries))

    print_guide(
        [
            "Structured Streaming의 각 토픽별 쿼리 실행 상태.",
            "",
            "칼럼 설명:",
            "  Batch : 해당 쿼리의 마이크로 배치 번호 (0부터 시작)",
            "  Rows  : 이번 배치에서 처리한 입력 레코드 수",
            "  In/s  : 초당 입력 레코드 수 (Kafka에서 읽은 속도)",
            "  Out/s : 초당 처리 레코드 수 (foreachBatch 처리 속도)",
            "",
            "Rows = 0인 배치:",
            "  availableNow=True 트리거에서 마지막 배치는 처리할 데이터가 없어",
            "  Rows=0으로 기록된다. 이는 정상이며, 이 시점에 쿼리가 종료된다.",
            "",
            f"  총 쿼리: {len(queries)}개, 성공: {succeeded}, 실패: {failed}",
            f"  평균 배치 수: {avg_batches:.1f} (Progress 이벤트는 마지막 배치만 기록될 수 있음)",
        ]
    )


# ---------------------------------------------------------------------------
# 9. Spark 설정 요약
# ---------------------------------------------------------------------------


def analyze_spark_config(events: list[dict]) -> None:
    print_section("9. Spark 설정")

    for e in events:
        if e.get("Event") != "SparkListenerEnvironmentUpdate":
            continue

        props = e.get("Spark Properties", {})
        keys_of_interest = [
            "spark.app.name",
            "spark.scheduler.mode",
            "spark.driver.memory",
            "spark.driver.cores",
            "spark.executor.memory",
            "spark.executor.cores",
            "spark.executor.instances",
            "spark.dynamicAllocation.enabled",
            "spark.sql.shuffle.partitions",
            "spark.sql.caseSensitive",
            "spark.sql.session.timeZone",
            "spark.sql.defaultCatalog",
            "spark.yarn.maxAppAttempts",
        ]

        print(f"  {'Key':<50} {'Value'}")
        print(f"  {'-' * 80}")
        for key in keys_of_interest:
            val = props.get(key, "N/A")
            print(f"  {key:<50} {val}")

        env_props = {k: v for k, v in props.items() if "kafka" in k.lower() or "KAFKA" in k}
        if env_props:
            print("\n  Kafka 관련 설정:")
            for k, v in sorted(env_props.items()):
                print(f"    {k}: {v}")

        print_guide(
            [
                "Spark 앱이 실행될 때 적용된 설정 값.",
                "",
                "주요 설정:",
                "  scheduler.mode        : FAIR이면 멀티스레드 토픽 간 리소스 공정 분배",
                "  executor.instances    : executor 수. concurrency보다 크거나 같아야 효율적",
                "  executor.cores        : executor당 코어 수. 높을수록 동시 task 처리 가능",
                "  executor.memory       : executor당 메모리. persist(MEMORY_AND_DISK)에 영향",
                "  maxAppAttempts        : 1이면 앱 실패 시 재시도 없이 종료",
                "  shuffle.partitions    : N/A면 기본값 200 사용",
                "",
                "  YARN 리소스 계산: instances × (cores + memory) = 앱의 총 YARN 점유량",
                "  예: 4 executor × (2 cores + 4GB) = 8 vCores + 16GB",
            ]
        )
        break


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <eventlog_dir>")
        sys.exit(1)

    eventlog_dir = Path(sys.argv[1])
    if not eventlog_dir.is_dir():
        print(f"Error: {eventlog_dir} is not a directory")
        sys.exit(1)

    zstd_files = list(eventlog_dir.glob("events_*.zstd"))
    if not zstd_files:
        print(f"Error: no events_*.zstd files found in {eventlog_dir}")
        sys.exit(1)

    print(f"Reading {len(zstd_files)} event log file(s) from {eventlog_dir.name}")
    lines = decompress_and_read(eventlog_dir)
    events = parse_events(lines)
    print(f"Parsed {len(events)} events")

    analyze_spark_config(events)
    analyze_pools(events)
    analyze_topic_timeline(events)
    analyze_sql_performance(events)
    analyze_task_skew(events)
    analyze_executors(events)
    analyze_memory(events)
    analyze_cpu(events)
    analyze_streaming(events)


if __name__ == "__main__":
    main()
