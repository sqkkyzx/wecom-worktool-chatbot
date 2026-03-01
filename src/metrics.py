import time
import logging
import psycopg
from prometheus_client import Counter, Gauge, Info, Histogram
from config import settings

# ==================== 1. 基础消息与系统指标 ====================
metrics_receive_msg_counter = Counter("worktool_received_messages_total", "接收到的消息总数", ["room_type", "text_type"])
metrics_db_error_counter = Counter("worktool_db_errors_total", "数据库操作错误总数")
metrics_worktool_send_error_counter = Counter("worktool_send_errors_total", "调用 Worktool 发送消息失败总数")
metrics_active_tasks_gauge = Gauge("worktool_active_tasks", "当前正在后台处理的 AI 回复任务数")

# ==================== 2. 大模型与业务逻辑指标 ====================
metrics_session_status_counter = Counter("worktool_session_status_total", "会话记忆命中状态", ["status"]) # 标签值: new, resumed, expired
metrics_dify_reply_length_counter = Counter("dify_reply_characters_total", "Dify 回复的总字符数")
metrics_dify_request_latency = Histogram(
    "dify_request_duration_seconds",
    "Dify 完整流式请求的耗时",
    buckets=[1.0, 5.0, 15.0, 30.0, 60.0, 120.0]
)

# ==================== 3. 抓取时更新的聚合指标 ====================
metrics_robot_online_status = Gauge("worktool_robot_online", "机器人在线状态（1=在线，0=离线）")
metrics_robot_info = Info("worktool_robot", "机器人信息")
metrics_group_msg_gauge = Gauge("worktool_group_messages", "各群组消息交互量计数", ["group_name"])

_last_metrics_update_time = 0

def update_custom_metrics(bot_instance):
    """更新自定义指标，带 60 秒本地缓存防抖"""
    global _last_metrics_update_time
    current_time = time.time()

    if current_time - _last_metrics_update_time < 60:
        return

    try:
        # 1. 收集机器人在线状态
        is_online = bot_instance.get_online_status()
        metrics_robot_online_status.set(1 if is_online else 0)

        robot_info = bot_instance.get_robot_info()
        if robot_info:
            metrics_robot_info.info({
                "robot_id": robot_info.get("robotId", ""),
                "name": robot_info.get("name", ""),
                "auth_expire": robot_info.get("authExpir", ""),
                "open_callback": str(robot_info.get("openCallback", ""))
            })

        # 2. 收集数据库侧聚合统计
        table_name = f"{settings.db_schema}.{settings.table_prefix}worktook_message"
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT group_name, COUNT(*) FROM {table_name} WHERE room_type IN (1, 3) GROUP BY group_name;")
                for row in cur.fetchall():
                    group_name, count = row
                    if group_name:
                        metrics_group_msg_gauge.labels(group_name=group_name).set(count)

        _last_metrics_update_time = current_time
    except Exception as e:
        logging.error(f"更新监控聚合指标失败: {e}")