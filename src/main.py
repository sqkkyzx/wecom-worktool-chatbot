import logging
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, BackgroundTasks, Depends
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from config import settings
from metrics import metrics_receive_msg_counter, metrics_db_error_counter, update_custom_metrics
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
    yield
    logging.info("服务关闭...")


app = FastAPI(lifespan=lifespan)


@app.get("/metrics", dependencies=[Depends(verify_metrics_auth)])
async def get_metrics():
    """Prometheus 监控指标抓取接口"""
    # 触发聚合类指标的本地更新
    update_custom_metrics(bot)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/message", response_model=WorktoolMessageResponse)
async def receive_message(msg: WorktoolMessageRequest, background_tasks: BackgroundTasks):
    """处理 Worktool 推送的消息入口"""
    # 增加接收流量打点
    metrics_receive_msg_counter.labels(room_type=str(msg.roomType), text_type=str(msg.textType)).inc()

    try:
        # 1. 持久化接收到的消息，并获取自增 ID
        msg_id = save_incoming_message(msg)

        # 2. 将 Dify 流式请求与发送回复的操作扔进后台任务队列
        need_reply = False
        if msg.textType in [1, 15]:  # 首先必须是文本类的消息
            # 解析唤醒词并检查
            has_wake_word = False
            if settings.wake_words and msg.spoken:
                wake_word_list = [w.strip() for w in settings.wake_words.split(',') if w.strip()]
                # 判断 msg.spoken 中是否包含任意一个唤醒词
                has_wake_word = any(word in msg.spoken for word in wake_word_list)

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