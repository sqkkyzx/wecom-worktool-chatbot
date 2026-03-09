import json
import logging
import time
import httpx
import psycopg
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from wecom_worktool import WorktoolAction
from config import settings

# 引入抽离的监控指标
from metrics import (
    metrics_worktool_send_error_counter,
    metrics_active_tasks_gauge,
    metrics_session_status_counter,
    metrics_dify_reply_length_counter,
    metrics_dify_request_latency
)


# ==================== 全局常量与实例 ====================
TABLE_MSG = f"{settings.db_schema}.{settings.table_prefix}worktool_message"
TABLE_MEMORY = f"{settings.db_schema}.{settings.table_prefix}worktool_memory"

# 在模块级别初始化为 None
http_client: httpx.AsyncClient | None = None

# 全局初始化 Worktool 机器人实例
bot = WorktoolAction(robot_id=settings.robot_id, key=settings.robot_key)
security = HTTPBasic()


# ==================== Pydantic 数据模型 ====================
class WorktoolMessageRequest(BaseModel):
    spoken: str = Field(default="", description="问题文本")
    rawSpoken: str = Field(default="", description="原始问题文本")
    receivedName: str = Field(default="", description="提问者名称")
    groupName: str = Field(default="", description="QA所在群名（群聊）")
    groupRemark: str = Field(default="", description="QA所在群备注名（群聊）")
    roomType: int = Field(default=4, description="QA所在房间类型 1=外部群 2=外部联系人 3=内部群 4=内部联系人")
    atMe: bool = Field(default=False, description="是否@机器人（群聊）")
    textType: int = Field(default=0, description="消息类型 0=未知 1=文本 2=图片 3=语音 5=视频 7=小程序 8=链接 9=文件 13=合并记录 15=带回复文本")
    fileBase64: Optional[str] = Field(default="")

class WorktoolMessageResponse(BaseModel):
    code: int = 0
    message: str = "success"


# ==================== 鉴权依赖 ====================
def verify_metrics_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """校验 Prometheus 抓取时的 Basic Auth 凭证"""
    correct_username = secrets.compare_digest(credentials.username, settings.metrics_user)
    correct_password = secrets.compare_digest(credentials.password, settings.metrics_password)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials


# ==================== 数据库操作 ====================
def init_database():
    """初始化数据库表结构"""
    logging.info("正在初始化数据库表结构...")
    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema};")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_MSG} (
                        id BIGSERIAL PRIMARY KEY,
                        spoken TEXT NOT NULL,
                        raw_spoken TEXT NOT NULL,
                        received_name VARCHAR(255) NOT NULL,
                        group_name VARCHAR(255) NOT NULL,
                        group_remark VARCHAR(255) NOT NULL,
                        room_type INTEGER NOT NULL,
                        at_me BOOLEAN NOT NULL,
                        text_type INTEGER NOT NULL,
                        file_base64 TEXT,
                        is_bot_reply BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_MEMORY} (
                        id BIGSERIAL PRIMARY KEY,
                        received_name VARCHAR(255) NOT NULL,
                        group_name VARCHAR(255) NOT NULL,
                        memory TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_memory_created_at ON {TABLE_MEMORY} (created_at);")
            conn.commit()
    except Exception as e:
        logging.error(f"数据库初始化失败: {e}")
        raise e

def save_incoming_message(msg: WorktoolMessageRequest) -> int:
    """持久化接收到的消息，并返回消息 ID"""
    with psycopg.connect(settings.db_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {TABLE_MSG} 
                (spoken, raw_spoken, received_name, group_name, group_remark, room_type, at_me, text_type, file_base64) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                msg.spoken, msg.rawSpoken, msg.receivedName,
                msg.groupName, msg.groupRemark, msg.roomType,
                msg.atMe, msg.textType, msg.fileBase64
            ))
            msg_id = cur.fetchone()[0]
        conn.commit()
    return msg_id

def _save_reply_to_db(request: WorktoolMessageRequest, full_reply: str):
    """持久化机器人回复消息到同一张表"""
    with psycopg.connect(settings.db_conn_uri) as conn:
        with conn.cursor() as cur:
            # 对于机器人的回复，直接借用 request 中的群信息，将发送者硬编码为机器人标识
            cur.execute(f"""
                INSERT INTO {TABLE_MSG} 
                (spoken, raw_spoken, received_name, group_name, group_remark, room_type, at_me, text_type, is_bot_reply) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
            """, (
                full_reply, full_reply, "我（AI）",
                request.groupName, request.groupRemark, request.roomType,
                False, 1  # 1=文本类型
            ))
        conn.commit()

def get_recent_group_history(group_name: str) -> str:
    """查询群组最近的消息（混合用户与Bot），同时受条数和字符数限制"""
    if not group_name:
        return ""

    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                # 倒序查询最近 limit_count 条记录，包含用户和 Bot
                cur.execute(f"""
                        SELECT created_at, received_name, raw_spoken, is_bot_reply 
                        FROM {TABLE_MSG} 
                        WHERE group_name = %s AND room_type IN (1, 3)
                        ORDER BY created_at DESC 
                        LIMIT %s
                    """, (group_name, settings.group_history_limit_count))

                rows = cur.fetchall()
                if not rows:
                    return ""

                history_lines = []
                current_length = 0

                # 遍历处理（此时顺序为：最新的消息在最前面）
                for row in rows:
                    created_at, received_name, raw_spoken, is_bot_reply = row
                    time_str = created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(created_at, 'strftime') else \
                    str(created_at).split('.')[0]

                    sender = "AI助手" if is_bot_reply else received_name
                    line = f"[{time_str}] <{sender}> 说：{raw_spoken}"

                    line_len = len(line) + 1  # +1 为换行符留足余量

                    # 触发字符长度限制，停止追溯更早的消息
                    if current_length + line_len > settings.group_history_limit_chars:
                        break

                    history_lines.append(line)
                    current_length += line_len

                # 翻转数组，使输出符合人类直觉（旧消息在上，新消息在下）
                history_lines.reverse()
                return "\n".join(history_lines)
    except Exception as e:
        logging.error(f"查询群组历史消息失败: {e}")
        return ""


def get_history_by_days(received_name: str, group_name: str, days: int) -> str:
    """精准提取特定用户在特定域内的聊天记录"""
    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                query = f"""
                    SELECT created_at, raw_spoken 
                    FROM {TABLE_MSG} 
                    WHERE group_name = %s AND received_name = %s AND is_bot_reply = FALSE 
                    AND created_at >= CURRENT_DATE - INTERVAL '{days} days'
                    ORDER BY created_at
                """
                cur.execute(query, (group_name, received_name))
                rows = cur.fetchall()
                if not rows: return ""

                history_lines = []
                for row in rows:
                    created_at, raw_spoken = row
                    time_str = created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(created_at, 'strftime') else str(created_at).split('.')[0]
                    history_lines.append(f"[{time_str}] {received_name} 说：{raw_spoken}")
                return "\n".join(history_lines)
    except Exception as e:
        logging.error(f"提取历史失败: {e}")
        return ""


def get_latest_memory(received_name: str, group_name: str) -> str:
    """查询指定用户在指定域（群/私聊）的最新记忆"""
    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT memory FROM {TABLE_MEMORY} 
                    WHERE received_name = %s AND group_name = %s
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (received_name, group_name))
                row = cur.fetchone()
                return row[0] if row else ""
    except Exception as e:
        logging.error(f"查询最新记忆失败: {e}")
        return ""

def save_new_memory(received_name: str, group_name: str, memory_text: str):
    """保存域隔离的新记忆"""
    if not memory_text: return
    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {TABLE_MEMORY} (received_name, group_name, memory) 
                    VALUES (%s, %s, %s)
                """, (received_name, group_name, memory_text))
            conn.commit()
    except Exception as e:
        logging.error(f"保存记忆失败: {e}")


def get_active_users_in_group(group_name: str, days: int) -> list:
    """按 group_name 聚合，找出近期发过言的所有 received_name"""
    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT DISTINCT received_name 
                    FROM {TABLE_MSG} 
                    WHERE group_name = %s AND is_bot_reply = FALSE 
                    AND created_at >= CURRENT_DATE - INTERVAL '{days} days'
                """, (group_name,))
                rows = cur.fetchall()
                return [row[0] for row in rows] if rows else []
    except Exception as e:
        logging.error(f"查询活跃用户失败: {e}")
        return []


# ==================== AI 请求与回复核心业务 ====================
def _send_worktool_chunk(request: WorktoolMessageRequest, text: str):
    """辅助函数：执行单次 Worktool 发送"""
    if not text:
        return
    try:
        with bot.action as action:
            if request.roomType in [1, 3]:  # 群聊模式
                action.send_text(receiver=[request.groupRemark or request.groupName], msg=text)
            else:  # 单聊模式
                action.send_text(receiver=[request.receivedName], msg=text)
    except Exception as e:
        logging.error(f"向 Worktool 推送消息块失败: {e}")
        metrics_worktool_send_error_counter.inc()

async def process_and_reply(msg_id: int, request: WorktoolMessageRequest):
    """后台任务：获取历史会话 -> 流式请求 Dify -> 按换行符分段回复"""
    with metrics_active_tasks_gauge.track_inprogress():
        user_identifier = f"{request.receivedName}__{request.groupName}" if request.groupName else request.receivedName
        full_reply = ""
        buffer = ""
        conversation_id = None

        global http_client
        if http_client is None:
            logging.error("HTTP Client 未初始化！")
            return

        try:
            # 1. 获取最新历史会话 ID
            logging.info(f"正在获取用户 {user_identifier} 的历史会话...")
            try:
                conv_res = await http_client.get(
                    f"{settings.dify_url}/conversations?user={user_identifier}&limit=1",
                    headers={"Authorization": f"Bearer {settings.dify_token}"}
                )
                conv_res.raise_for_status()
                conversations = conv_res.json().get("data", [])

                if conversations:
                    conversation = conversations[0]
                    conversation_id = conversation["id"]
                    if time.time() - conversation["updated_at"] > settings.conversation_expire:
                        logging.warning(f"用户 {user_identifier} 的会话已过期，将开启新对话。")
                        metrics_session_status_counter.labels(status="expired").inc()
                        conversation_id = None
                    else:
                        logging.info(f"已恢复用户 {user_identifier} 的会话: {conversation_id}")
                        metrics_session_status_counter.labels(status="resumed").inc()
                else:
                    logging.info(f"用户 {user_identifier} 无历史会话，将开启新对话。")
                    metrics_session_status_counter.labels(status="new").inc()
            except Exception as e:
                logging.warning(f"获取历史会话失败，回退到无会话模式: {e}")

            latest_memory = await run_in_threadpool(get_latest_memory, request.receivedName, request.groupName)

            # === 提取群聊上下文 ===
            group_history = ""
            if request.roomType in [1, 3] and request.groupName:
                # 使用线程池执行同步的数据库阻塞操作
                group_history = await run_in_threadpool(get_recent_group_history, request.groupName)

            # 2. 组装请求体并开始流式请求
            payload = {
                "inputs": {
                    "rawSpoken": request.rawSpoken,
                    "receivedName": request.receivedName,
                    "groupName": request.groupName,
                    "groupRemark": request.groupRemark,
                    "atMe": request.atMe,
                    "roomType": request.roomType,
                    "fileBase64": request.fileBase64,
                    "groupHistory": group_history,
                    "latestMemory": latest_memory
                },
                "query": request.spoken,
                "user": user_identifier,
                "response_mode": "streaming"
            }
            if conversation_id:
                payload["conversation_id"] = conversation_id

            with metrics_dify_request_latency.time():
                async with http_client.stream(
                    method="POST",
                    url=f"{settings.dify_url}/chat-messages",
                    headers={"Authorization": f"Bearer {settings.dify_token}"},
                    json=payload
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if not line or not line.startswith("data:"):
                                continue

                        raw_data = line[len("data:"):].strip()
                        if not raw_data:
                            continue

                        try:
                            event_data = json.loads(raw_data)
                            event = event_data.get('event')

                            if event in ["message", "agent_message"]:
                                answer_chunk = event_data.get('answer', '')
                                full_reply += answer_chunk
                                buffer += answer_chunk

                                if '\n' in buffer:
                                    parts = buffer.split('\n')
                                    for part in parts[:-1]:
                                        part_clean = part.strip()
                                        if part_clean:
                                            await run_in_threadpool(_send_worktool_chunk, request, part_clean)
                                    buffer = parts[-1]

                            elif event == "message_end":
                                if buffer.strip():
                                    await run_in_threadpool(_send_worktool_chunk, request, buffer.strip())
                                buffer = ""

                            elif event == "error":
                                error_msg = event_data.get('message', '未知错误')
                                logging.error(f"Dify 返回流式错误: {error_msg}")
                                await run_in_threadpool(_send_worktool_chunk, request, f"[请求异常] {error_msg}")

                        except json.JSONDecodeError:
                            logging.warning(f"无法解析的 SSE 数据行: {raw_data}")
                            continue

            # 3. 完整回复存入本地数据库
            if full_reply:
                metrics_dify_reply_length_counter.inc(len(full_reply))
                await run_in_threadpool(_save_reply_to_db, request, full_reply)

        except Exception as e:
            logging.error(f"处理流式并回复消息异常: {e}", exc_info=True)
            await run_in_threadpool(_send_worktool_chunk, request, "抱歉，连接大模型服务时出现了异常。")


async def _run_single_memory_workflow(received_name: str, group_name: str, days: int) -> bool:
    """内部函数：处理单个用户的记忆生成请求"""
    global http_client
    if not settings.dify_token_for_memory:
        return False

    history_text = await run_in_threadpool(get_history_by_days, received_name, group_name, days)
    if not history_text:
        return False

    latest_memory = await run_in_threadpool(get_latest_memory, received_name, group_name)
    user_identifier = f"{received_name}__{group_name}"

    payload = {
        "inputs": {
            "history": history_text,
            "latest_memory": latest_memory,
            "receivedName": received_name,
            "groupName": group_name
        },
        "response_mode": "blocking",
        "user": user_identifier
    }

    try:
        res = await http_client.post(
            f"{settings.dify_url}/workflows/run",
            headers={"Authorization": f"Bearer {settings.dify_token_for_memory}"},
            json=payload,
            timeout=120.0
        )
        res.raise_for_status()
        data = res.json()

        outputs = data.get("data", {}).get("outputs", {})
        new_memory = outputs.get("memory", outputs.get("text", outputs.get("result", "")))
        if not new_memory and outputs:
            new_memory = str(list(outputs.values())[0])

        if new_memory:
            await run_in_threadpool(save_new_memory, received_name, group_name, new_memory)
            return True
    except Exception as e:
        logging.error(f"提取 {received_name} 记忆失败: {e}")
    return False


async def execute_batch_memory_workflow(request: WorktoolMessageRequest, target_group: str, days: int):
    """主任务：扫描目标域的所有活跃用户，逐一触发记忆提取"""
    users = await run_in_threadpool(get_active_users_in_group, target_group, days)

    if not users:
        await run_in_threadpool(_send_worktool_chunk, request,
                                f"⚠️ 未找到域【{target_group}】在近 {days} 天的活跃用户记录。")
        return

    await run_in_threadpool(_send_worktool_chunk, request,
                            f"🔍 域【{target_group}】找到 {len(users)} 位活跃用户，开始逐一生成记忆画像...")

    success_count = 0
    for user in users:
        # 串行执行，避免瞬间高并发打满 Dify 并发限制
        is_success = await _run_single_memory_workflow(user, target_group, days)
        if is_success:
            success_count += 1

    await run_in_threadpool(_send_worktool_chunk, request,
                            f"🎉 域【{target_group}】记忆提取完毕！成功更新 {success_count}/{len(users)} 人的画像。")