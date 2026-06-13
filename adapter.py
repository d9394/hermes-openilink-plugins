from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    AIOHTTP_AVAILABLE = False

# 注意：不再从核心导入 Platform 枚举，保持核心代码零侵入 (Zero changes)
from gateway.config import PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_url,
    safe_url_for_log,
)

# ------------------------------------------------------------------
# Plugin Hooks (插件专用钩子函数)
# ------------------------------------------------------------------

def apply_yaml_config(yaml_cfg: dict, platform_cfg: PlatformConfig) -> Optional[dict]:
    """
    将用户的 config.yaml 配置项转换为环境变量，或直接注入到 platform_cfg.extra 中。
    允许插件拥有自己独立的配置架构，而不破坏核心 gateway/config.py。
    """
    extra_updates = {}
    openilink_cfg = yaml_cfg.get("openilink", {})
    
    if "token" in openilink_cfg and not os.getenv("OPENILINK_TOKEN"):
        os.environ["OPENILINK_TOKEN"] = str(openilink_cfg["token"])
        
    if "hub_url" in openilink_cfg and not os.getenv("OPENILINK_HUB_URL"):
        os.environ["OPENILINK_HUB_URL"] = str(openilink_cfg["hub_url"])
        extra_updates["hub_url"] = openilink_cfg["hub_url"]
        
    return extra_updates


def env_enablement() -> Optional[dict]:
    """
    在适配器实例化之前从环境变量中种子化配置。
    以此确保仅有环境变量的用户设置在执行 `hermes gateway status` 时能正确显示。
    """
    token = os.getenv("OPENILINK_TOKEN", "")
    hub_url = os.getenv("OPENILINK_HUB_URL", "https://localhost:9800")
    home_channel = os.getenv("OPENILINK_HOME_CHANNEL", "")
    
    if not token:
        return None
        
    result = {
        "extra": {"hub_url": hub_url},
        "token": token
    }
    if home_channel:
        result["home_channel"] = {"chat_id": home_channel}
    return result


# ------------------------------------------------------------------
# Adapter Implementation (适配器核心实现)
# ------------------------------------------------------------------

class OpeniLinkAdapter(BasePlatformAdapter):
    """通过 WebSocket 连接到 OpeniLink Hub 以收发消息的 Hermes 插件适配器。"""

    _MAX_RECONNECT_ATTEMPTS = 10
    _BASE_DELAY = 2.0
    _MAX_DELAY = 60.0

    def __init__(self, config: PlatformConfig):
        # 传入字符串标识 "openilink" 作为平台名，底层基类会自动将其处理为动态平台
        super().__init__(config, "openilink")

        hub_url = (config.extra.get("hub_url") or "").rstrip("/")
        if not hub_url:
            hub_url = "https://localhost:9800"
            
        ws_scheme = "wss" if hub_url.startswith("https") else "ws"
        http_base = hub_url.replace("https://", "").replace("http://", "")
        self._ws_url = f"{ws_scheme}://{http_base}/bot/v1/ws"

        self._token: str = config.token or os.getenv("OPENILINK_TOKEN", "")
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._reconnect_attempts: int = 0
        self._bot_id: str = ""
        self._installation_id: str = ""

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.error("[openilink] Cannot connect: aiohttp is not installed.")
            return False
        if not self._token:
            logger.error("[openilink] Cannot connect: OPENILINK_TOKEN is missing.")
            return False
            
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()

            url = f"{self._ws_url}?token={self._token}"
            logger.info("[openilink] Connecting to %s", safe_url_for_log(url))
            self._ws = await self._session.ws_connect(url, heartbeat=50)

            self._reconnect_attempts = 0
            self._receive_task = asyncio.create_task(self._receive_loop())
            self._mark_connected()
            logger.info("[openilink] Connected successfully")
            return True
        except Exception as exc:
            logger.error("[openilink] Connection failed: %s", exc)
            return False

    async def disconnect(self) -> None:
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

        self._mark_disconnected()
        logger.info("[openilink] Disconnected")

    async def _receive_loop(self) -> None:
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("[openilink] Invalid JSON received: %s", msg.data[:120])
                        continue
                    await self._dispatch(payload)
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    logger.warning("[openilink] WebSocket disconnected via %s", msg.type.name)
                    break
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("[openilink] Error in receive loop: %s", exc)

        await self._reconnect()

    async def _dispatch(self, payload: dict) -> None:
        msg_type = payload.get("type", "")

        if msg_type == "init":
            data = payload.get("data", {})
            self._bot_id = data.get("bot_id", "")
            self._installation_id = data.get("installation_id", "")
            logger.info("[openilink] Init complete: bot=%s, installation=%s", self._bot_id, self._installation_id)
        elif msg_type == "event":
            await self._handle_event(payload)
        elif msg_type == "ack":
            if not payload.get("ok", False):
                logger.warning("[openilink] ACK failed for req_id=%s", payload.get("req_id", ""))
        elif msg_type == "error":
            logger.error("[openilink] Server error message: %s", payload.get("error", ""))

    async def _handle_event(self, payload: dict) -> None:
        event_data = payload.get("event", {})
        event_type = event_data.get("type", "")
        data = event_data.get("data", {})

        sender = data.get("sender", {})
        user_id = sender.get("id", "")
        group = data.get("group")
        items = data.get("items", [])
        message_id = data.get("message_id", "")

        chat_id = group.get("id") if group else user_id
        if not chat_id:
            return

        chat_type = "group" if group else "dm"
        text_parts = []
        media_urls = []
        media_types = []

        for item in items:
            item_type = item.get("type", "")
            if item_type == "text":
                text_parts.append(item.get("text", ""))
            elif item_type in ("image", "video", "file", "voice"):
                media = item.get("media", {})
                media_url = media.get("url", "")
                if media_url:
                    try:
                        ext = ".jpg" if item_type == "image" else f".{item_type}"
                        cached = await cache_image_from_url(media_url, ext=ext)
                        media_urls.append(cached)
                        media_types.append(f"{item_type}/{ext.lstrip('.')}")
                    except Exception as exc:
                        logger.warning("[openilink] Failed to cache media: %s", exc)

        text = "\n".join(text_parts)

        if event_type == "message.image" or media_urls:
            msg_type = MessageType.PHOTO
        elif event_type == "message.voice":
            msg_type = MessageType.VOICE
        elif event_type == "message.video":
            msg_type = MessageType.VIDEO
        elif event_type == "message.file":
            msg_type = MessageType.DOCUMENT
        else:
            msg_type = MessageType.TEXT

        source = self.build_source(
            chat_id=chat_id,
            chat_type=chat_type,
            user_id=user_id,
        )

        event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=payload,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
        )
        await self.handle_message(event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._ws or self._ws.closed:
            return SendResult(success=False, error="WebSocket not connected", retryable=True)

        try:
            chunks = self.truncate_message(content, max_length=4096)
            last_req_id = ""
            for chunk in chunks:
                req_id = f"r_{uuid.uuid4().hex[:8]}"
                await self._ws.send_json({
                    "type": "send",
                    "req_id": req_id,
                    "to": chat_id,
                    "content": chunk,
                })
                last_req_id = req_id
            return SendResult(success=True, message_id=last_req_id)
        except Exception as exc:
            logger.error("[openilink] Send failed: %s", exc)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if not self._ws or self._ws.closed:
            return
        try:
            await self._ws.send_json({"type": "send_typing", "to": chat_id})
        except Exception:
            pass

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        text = f"![image]({image_url})"
        if caption:
            text = f"{caption}\n{text}"
        return await self.send(chat_id, text, reply_to=reply_to, metadata=metadata)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}

    async def _reconnect(self) -> None:
        if self._reconnect_attempts >= self._MAX_RECONNECT_ATTEMPTS:
            msg = f"Reconnection exhausted after {self._MAX_RECONNECT_ATTEMPTS} attempts"
            logger.error("[openilink] %s", msg)
            self._set_fatal_error("openilink_reconnect", msg, retryable=True)
            return

        self._reconnect_attempts += 1
        delay = min(self._BASE_DELAY * (2 ** (self._reconnect_attempts - 1)), self._MAX_DELAY)
        logger.warning(
            "[openilink] Reconnecting in %.0fs (attempt %d/%d)",
            delay, self._reconnect_attempts, self._MAX_RECONNECT_ATTEMPTS,
        )
        await asyncio.sleep(delay)
        await self.connect()


# ------------------------------------------------------------------
# Plugin Entry Point (插件向系统注册自己唯一的入口)
# ------------------------------------------------------------------

def register(ctx: Any) -> None:
    """让 Hermes 核心识别并加载 OpeniLink 平台网关"""
    ctx.register_platform("openilink", OpeniLinkAdapter)
