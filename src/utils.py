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
TABLE_MSG = f"{settings.db_schema}.{settings.table_prefix}worktook_message"
TABLE_REPLY = f"{settings.db_schema}.{settings.table_prefix}worktook_reply"
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
                # 消息接收表
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
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                # 机器人回复表
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_REPLY} (
                        id BIGSERIAL PRIMARY KEY,
                        msg_id BIGINT REFERENCES {TABLE_MSG}(id) ON DELETE CASCADE,
                        reply_content TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
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


def get_recent_group_history(group_name: str) -> str:
    """查询群组最近的消息并格式化输出"""
    if not group_name:
        return ""

    try:
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                # 倒序查询最近 limit 条记录
                cur.execute(f"""
                    SELECT created_at, received_name, raw_spoken 
                    FROM {TABLE_MSG} 
                    WHERE group_name = %s AND room_type IN (1, 3)
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (group_name, settings.group_history_limit))

                rows = cur.fetchall()

                if not rows:
                    return ""

                # 查出来是按时间倒序的（最新的在前面），反转以恢复正常时间流
                rows.reverse()

                history_lines = []
                for row in rows:
                    created_at, received_name, raw_spoken = row
                    # 格式化时间，去掉微秒和时区，让输出更干净
                    time_str = created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(created_at, 'strftime') else str(
                        created_at)
                    history_lines.append(f"[{time_str}] <{received_name}> 说：{raw_spoken}")

                return "\n".join(history_lines)
    except Exception as e:
        logging.error(f"查询群组历史消息失败: {e}")
        return ""

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

def _save_reply_to_db(msg_id: int, full_reply: str):
    with psycopg.connect(settings.db_conn_uri) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {TABLE_REPLY} (msg_id, reply_content) 
                VALUES (%s, %s)
            """, (msg_id, full_reply))
        conn.commit()

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
                    "groupHistory": group_history
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
                await run_in_threadpool(_save_reply_to_db, msg_id, full_reply)

        except Exception as e:
            logging.error(f"处理流式并回复消息异常: {e}", exc_info=True)
            await run_in_threadpool(_send_worktool_chunk, request, "抱歉，连接大模型服务时出现了异常。")