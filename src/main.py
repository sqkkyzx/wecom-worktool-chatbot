import json
import logging
import time
import httpx
from contextlib import asynccontextmanager
from typing import Optional
import uvicorn

import secrets
from fastapi import Depends, HTTPException, status
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from pydantic import BaseModel, Field
import psycopg
from prometheus_client import Counter, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from wecom_worktool import WorktoolAction

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==================== 全局初始化 ====================
TABLE_MSG = f"{settings.db_schema}.{settings.table_prefix}worktook_message"
TABLE_REPLY = f"{settings.db_schema}.{settings.table_prefix}worktook_reply"

# 全局初始化 Worktool 机器人实例
bot = WorktoolAction(robot_id=settings.robot_id, key=settings.robot_key)

# ==================== Prometheus 监控指标 ====================
RECEIVE_MSG_COUNTER = Counter("worktool_received_messages_total", "接收到的消息总数", ["room_type", "text_type"])
DB_ERROR_COUNTER = Counter("worktool_db_errors_total", "数据库错误总数")

# 自定义监控指标 (供 /metrics 定期刷新)
ROBOT_ONLINE_STATUS = Gauge("worktool_robot_online", "机器人在线状态（1=在线，0=离线）")
ROBOT_INFO = Info("worktool_robot", "机器人信息")
GROUP_MSG_GAUGE = Gauge("worktool_group_messages", "群消息计数", ["group_name"])

# 用于控制指标刷新频率的全局时间戳
_last_metrics_update_time = 0


# ==================== Pydantic 模型 ====================
class WorktoolMessageRequest(BaseModel):
    spoken: str = Field(description="问题文本")
    rawSpoken: str = Field(description="原始问题文本")
    receivedName: str = Field(description="提问者名称")
    groupName: str = Field(description="QA所在群名（群聊）")
    groupRemark: str = Field(description="QA所在群备注名（群聊）")
    roomType: int = Field(description="QA所在房间类型 1=外部群 2=外部联系人 3=内部群 4=内部联系人")
    atMe: bool = Field(description="是否@机器人（群聊）")
    textType: int = Field(description="消息类型 0=未知 1=文本 2=图片 3=语音 5=视频 7=小程序 8=链接 9=文件 13=合并记录 15=带回复文本")
    fileBase64: Optional[str] = Field(default="")


class WorktoolMessageResponse(BaseModel):
    code: int = 0
    message: str = "success"


# ==================== Basic Auth ====================
security = HTTPBasic()


def verify_metrics_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """校验 Prometheus 抓取时的 Basic Auth 凭证"""
    # 使用 secrets.compare_digest 防止时序攻击
    correct_username = secrets.compare_digest(credentials.username, settings.metrics_user)
    correct_password = secrets.compare_digest(credentials.password, settings.metrics_password)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials


# ==================== 生命周期管理 (数据库初始化) ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("服务启动，正在初始化数据库...")
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
    yield


# ==================== FastAPI 实例 ====================
app = FastAPI(lifespan=lifespan)


# ==================== 核心逻辑 ====================

def update_custom_metrics():
    """更新自定义指标，带 60 秒本地缓存防抖"""
    global _last_metrics_update_time
    current_time = time.time()

    # 缓存 60 秒
    if current_time - _last_metrics_update_time < 60:
        return

    try:
        # 1. 收集 API 侧指标
        is_online = bot.get_online_status()
        ROBOT_ONLINE_STATUS.set(1 if is_online else 0)

        robot_info = bot.get_robot_info()
        if robot_info:
            ROBOT_INFO.info({
                "robot_id": robot_info.get("robotId", ""),
                "name": robot_info.get("name", ""),
                "auth_expire": robot_info.get("authExpir", ""),
                "open_callback": str(robot_info.get("openCallback", ""))
            })

        # 2. 收集 DB 侧指标 (例如：统计各群聊收到的消息总数)
        with psycopg.connect(settings.db_conn_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT group_name, COUNT(*) FROM {TABLE_MSG} WHERE room_type IN (1, 3) GROUP BY group_name;")
                for row in cur.fetchall():
                    group_name, count = row
                    if group_name:
                        GROUP_MSG_GAUGE.labels(group_name=group_name).set(count)

        _last_metrics_update_time = current_time
    except Exception as e:
        logging.error(f"更新自定义监控指标失败: {e}")


def _send_worktool_chunk(request: WorktoolMessageRequest, text: str):
    """辅助函数：执行单次 Worktool 发送"""
    if not text:
        return

    try:
        with bot.Action() as action:
            if request.roomType in [1, 3]:  # 群聊模式
                action.send_text(
                    receiver=[request.groupName or request.groupRemark],
                    msg=text,
                    # 考虑到分段发送，如果每句话都 @ 会很烦人，你可以根据需求决定后续片段是否继续 @
                    # at_list=[request.receivedName]
                )
            else:  # 单聊模式
                action.send_text(
                    receiver=[request.receivedName],
                    msg=text
                )
    except Exception as e:
        logging.error(f"向 Worktool 推送消息块失败: {e}")


async def process_and_reply(msg_id: int, request: WorktoolMessageRequest):
    """后台任务：获取历史会话 -> 流式请求 Dify -> 按换行符分段回复"""

    # 构建 Dify 识别的用户唯一标识
    user_identifier = f"{request.receivedName}__{request.groupName}" if request.groupName else request.receivedName

    full_reply = ""
    buffer = ""
    conversation_id = None  # 默认无会话

    # 针对流式响应，延长读超时限制
    timeout_config = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)

    try:
        async with httpx.AsyncClient(timeout=timeout_config) as client:

            # ==================== 1. 获取最新历史会话 ID ====================
            logging.info(f"正在获取用户 {user_identifier} 的历史会话...")
            try:
                conv_res = await client.get(
                    f"{settings.dify_url}/conversations?user={user_identifier}&limit=1",
                    headers={"Authorization": f"Bearer {settings.dify_token}"}
                )
                conv_res.raise_for_status()
                conversations = conv_res.json().get("data", [])

                if conversations:
                    # 取最近的一次会话
                    conversation = conversations[0]
                    conversation_id = conversation["id"]
                    conversation_updated_at = conversation["updated_at"]
                    if time.time() - conversation_updated_at > settings.conversation_expire:
                        logging.warning(f"用户 {user_identifier} 的会话已过期，将开启新对话。")
                        conversation_id = None
                    else:
                        logging.info(f"已恢复用户 {user_identifier} 的会话: {conversation_id}")
                else:
                    logging.info(f"用户 {user_identifier} 无历史会话，将开启新对话。")
            except Exception as e:
                logging.warning(f"获取历史会话失败 (可能由于网络或认证问题)，回退到无会话模式: {e}")

            # ==================== 2. 组装请求体并开始流式请求 ====================
            payload = {
                "inputs": {
                    "rawSpoken": request.rawSpoken,
                    "receivedName": request.receivedName,
                    "groupName": request.groupName,
                    "groupRemark": request.groupRemark,
                    "atMe": request.atMe,
                    "roomType": request.roomType,
                    "fileBase64": request.fileBase64
                },
                "query": request.spoken,
                "user": user_identifier,
                "response_mode": "streaming"
            }
            # 只有当成功获取到会话 ID 时，才加入 payload
            if conversation_id:
                payload["conversation_id"] = conversation_id

            async with client.stream(
                    method="POST",
                    url=f"{settings.dify_url}/chat-messages",
                    headers={"Authorization": f"Bearer {settings.dify_token}"},
                    json=payload
            ) as response:
                response.raise_for_status()

                # 异步迭代流式响应行
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue

                    raw_data = line[len("data:"):].strip()
                    if not raw_data:
                        continue

                    try:
                        event_data = json.loads(raw_data)
                        event = event_data.get('event')

                        # 处理消息块
                        if event in ["message", "agent_message"]:
                            answer_chunk = event_data.get('answer', '')
                            full_reply += answer_chunk
                            buffer += answer_chunk

                            # 一旦缓冲区出现换行符，就进行切割发送
                            if '\n' in buffer:
                                parts = buffer.split('\n')
                                for part in parts[:-1]:
                                    part_clean = part.strip()
                                    if part_clean:
                                        _send_worktool_chunk(request, part_clean)

                                buffer = parts[-1]

                        # 处理流结束
                        elif event == "message_end":
                            if buffer.strip():
                                _send_worktool_chunk(request, buffer.strip())
                            buffer = ""

                        elif event == "error":
                            error_msg = event_data.get('message', '未知错误')
                            logging.error(f"Dify 返回流式错误: {error_msg}")
                            _send_worktool_chunk(request, f"[请求异常] {error_msg}")

                    except json.JSONDecodeError:
                        logging.warning(f"无法解析的 SSE 数据行: {raw_data}")
                        continue

        # ==================== 3. 完整回复存入本地数据库 ====================
        if full_reply:
            with psycopg.connect(settings.db_conn_uri) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {TABLE_REPLY} (msg_id, reply_content) 
                        VALUES (%s, %s)
                    """, (msg_id, full_reply))
                conn.commit()

    except Exception as e:
        logging.error(f"处理流式并回复消息异常: {e}", exc_info=True)
        _send_worktool_chunk(request, "抱歉，连接大模型服务时出现了异常。")


@app.get("/metrics", dependencies=[Depends(verify_metrics_auth)])
async def get_metrics():
    """Prometheus 监控指标抓取接口"""
    # 每次 Prometheus 来拉取数据时，尝试更新自定义指标（内置 60s 缓存）
    update_custom_metrics()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/message", response_model=WorktoolMessageResponse)
async def receive_message(msg: WorktoolMessageRequest, background_tasks: BackgroundTasks):
    """处理 Worktool 推送的消息"""
    RECEIVE_MSG_COUNTER.labels(room_type=str(msg.roomType), text_type=str(msg.textType)).inc()

    try:
        # 1. 持久化接收到的消息，并获取自增 ID
        msg_id = None
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

        # 2. 将 Dify 请求与发送回复的逻辑放入后台任务
        # 只有在文本消息且被@（或者单聊）的情况下才触发回复
        if (msg.textType == 1 or msg.textType == 15)and (msg.atMe or msg.roomType in [2, 4]):
            background_tasks.add_task(process_and_reply, msg_id, msg)

        # 3. 立即响应 WeCom 回调，避免触发超时重传
        return WorktoolMessageResponse(code=0, message="success")

    except Exception as e:
        logging.error(f"消息入库失败: {e}", exc_info=True)
        DB_ERROR_COUNTER.inc()
        return WorktoolMessageResponse(code=-1, message=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)