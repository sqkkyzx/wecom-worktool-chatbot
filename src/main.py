import logging
import re
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, BackgroundTasks, Depends, Request
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from config import settings
from metrics import metrics_receive_msg_counter, metrics_db_error_counter, update_custom_metrics
import utils
from utils import (
    bot, verify_metrics_auth, init_database, save_incoming_message, process_and_reply,
    WorktoolMessageRequest, WorktoolMessageResponse
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期钩子：服务启动时触发初始化"""
    logging.info("服务启动...")
    init_database()
    # 启动时初始化，开启 Keep-Alive 复用
    timeout_config = httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)
    utils.http_client = httpx.AsyncClient(timeout=timeout_config)
    logging.info("全局 HTTP Client 已初始化")
    yield
    # 2. 关闭时：优雅释放所有保持的 TCP 连接
    if utils.http_client:
        await utils.http_client.aclose()
        logging.info("全局 HTTP Client 已关闭")
    logging.info("服务关闭...")


app = FastAPI(lifespan=lifespan)


@app.get("/metrics", dependencies=[Depends(verify_metrics_auth)])
async def get_metrics():
    """Prometheus 监控指标抓取接口"""
    # 触发聚合类指标的本地更新
    update_custom_metrics(bot)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/message", response_model=WorktoolMessageResponse)
async def receive_message(request: Request, background_tasks: BackgroundTasks):
    """处理 Worktool 推送的消息入口"""
    # === 新增：读取并打印原始 Payload ===
    try:
        raw_body = await request.body()
        if settings.debug:
            logging.info(f"收到消息：{raw_body.decode('utf-8')}")
        msg = WorktoolMessageRequest.model_validate_json(raw_body)
    except Exception as e:
        logging.error(f"解析原始 Payload 失败: {e}", exc_info=True)
        return WorktoolMessageResponse(code=-1, message="Payload parsing error")

    # 增加接收流量打点
    metrics_receive_msg_counter.labels(room_type=str(msg.roomType), text_type=str(msg.textType)).inc()
    logging.info(f"收到 {msg.groupName}/{msg.receivedName} 的消息")
    try:
        # 1. 持久化接收到的消息，并获取自增 ID
        msg_id = save_incoming_message(msg)

        # 2. 将 Dify 流式请求与发送回复的操作扔进后台任务队列
        need_reply = False
        if msg.textType in [1, 15]:  # 首先必须是文本类的消息
            # === 新增逻辑：拦截 Owner 的预留命令 ===
            # 判断条件：配置了 owner，且群名和群备注都等于 owner，且消息体以 / 开头
            if (settings.owner and
                    msg.groupName == settings.owner_group and
                    msg.receivedName == settings.owner and
                    msg.spoken.startswith("/")):

                logging.info(f"拦截到 Owner 控制台命令，当前跳过 AI 回复: {msg.spoken}")
                # TODO: 以后可以在这里增加 execute_command(msg.spoken) 函数
                # 正则：/记忆 [目标群组/私聊名] [天数]d
                memory_match = re.match(r"^/记忆\s+(\S+)\s+(\d+)d", msg.spoken.strip())
                if memory_match:
                    target_group = memory_match.group(1)
                    days = int(memory_match.group(2))
                    # 抛入批量处理队列
                    background_tasks.add_task(utils.execute_batch_memory_workflow, msg, target_group, days)
                else:
                    logging.info(f"未知的 Owner 命令或格式错误: {msg.spoken}")
            else:
                # 唤醒词解析与常规回复判定逻辑
                has_wake_word = False
                if settings.wake_words and msg.rawSpoken:
                    wake_word_list = [w.strip() for w in settings.wake_words.split(',') if w.strip()]
                    has_wake_word = any(word in msg.rawSpoken for word in wake_word_list)

                # 触发条件：
                # 被 @ 了或消息体里包含了唤醒词
                if msg.atMe or has_wake_word:
                    need_reply = True
                # 单聊
                elif msg.roomType in [2, 4]:
                    need_reply = True

        if need_reply:
            # 触发 Dify 流式请求
            background_tasks.add_task(process_and_reply, msg_id, msg)

        # 3. 立即响应 WeCom 回调 HTTP 200，避免企微网关超时重试
        return WorktoolMessageResponse(code=0, message="success")

    except Exception as e:
        logging.error(f"消息入库或处理分发失败: {e}", exc_info=True)
        metrics_db_error_counter.inc()
        return WorktoolMessageResponse(code=-1, message=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)