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
# 修复版的鸭子类型伪装器：支持哈希，解决 unhashable 报错
# ------------------------------------------------------------------
class PlatformFakeEnum:
    """伪装成 Platform 枚举对象，支持哈希和字符串值返回"""
    def __init__(self, val: str):
        self._val = val

    @property
    def value(self) -> str:
        return self._val

    def __str__(self) -> str:
        return self._val

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PlatformFakeEnum):
            return self._val == other._val
        return str(other) == self._val

    def __hash__(self) -> int:
        # 【核心修复】返回字符串的哈希，这样就能完美混入 {} 集合或 dict 的 key 中
        return hash(self._val)


# ------------------------------------------------------------------
# 内部钩子函数实现 (由 register 传入，桥接系统机制)
# ------------------------------------------------------------------

def check_openilink_requirements() -> bool:
    """检查依赖与关键环境变量是否就绪"""
    if not AIOHTTP_AVAILABLE:
        return False
    return bool(os.getenv("OPENILINK_TOKEN"))


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: PlatformConfig) -> Optional[dict]:
    """接管主 config.yaml 中 openilink 块的解析，并将其动态映射为环境变量"""
    extra_updates = {}
    openilink_cfg = yaml_cfg.get("openilink", {})
    
    if "token" in openilink_cfg and not os.getenv("OPENILINK_TOKEN"):
        os.environ["OPENILINK_TOKEN"] = str(openilink_cfg["token"])
        
    if "hub_url" in openilink_cfg and not os.getenv("OPENILINK_HUB_URL"):
        os.environ["OPENILINK_HUB_URL"] = str(openilink_cfg["hub_url"])
        extra_updates["hub_url"] = openilink_cfg["hub_url"]
        
    if "allow_all_users" in openilink_cfg and not os.getenv("OPENILINK_ALLOW_ALL_USERS"):
        os.environ["OPENILINK_ALLOW_ALL_USERS"] = str(openilink_cfg["allow_all_users"]).lower()
        
    return extra_updates


def _is_connected(adapter: OpeniLinkAdapter) -> bool:
    """供系统状态监测指令（如 hermes gateway status）检查连接可用性"""
    return adapter._ws is not None and not adapter._ws.closed

# ------------------------------------------------------------------
# OpeniLink 适配器核心类实现
# ------------------------------------------------------------------

class OpeniLinkAdapter(BasePlatformAdapter):
    """通过 WebSocket 连接到 OpeniLink Hub 架构的消息适配器插件。"""

    _MAX_RECONNECT_ATTEMPTS = 10
    _BASE_DELAY = 2.0
    _MAX_DELAY = 60.0

    def __init__(self, config: PlatformConfig):
        # 1. 传入支持哈希的伪装枚举对象
        fake_platform = PlatformFakeEnum("openilink")
        super().__init__(config, fake_platform)  # type: ignore

        # 2. 显式确保我们的实例属性也是这个伪装对象
        self.platform = fake_platform

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

    async def connect(self) -> bool:
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
            logger.info("[openilink] Init: bot=%s, installation=%s", data.get("bot_id", ""), data.get("installation_id", ""))
        elif msg_type == "event":
            await self._handle_event(payload)
        elif msg_type == "ack" and not payload.get("ok", False):
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

        source = self.build_source(chat_id=chat_id, chat_type=chat_type, user_id=user_id)
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
        if self._ws and not self._ws.closed:
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
# 模仿 Discord 规范的全新动态平台注册器
# ------------------------------------------------------------------

def register(ctx: Any) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="openilink",
        label="OpeniLink Hub",
        adapter_factory=OpeniLinkAdapter,
        check_fn=check_openilink_requirements,
        is_connected=_is_connected,
        required_env=["OPENILINK_TOKEN"],
        
        # 绑定 YAML 转换桥梁钩子
        apply_yaml_config_fn=_apply_yaml_config,
        
        # 将全局允许变量绑定到内核的鉴权机制
        allow_all_env="OPENILINK_ALLOW_ALL_USERS",
        
        # Cron 定时投递的专属默认环境变量
        cron_deliver_env_var="OPENILINK_HOME_CHANNEL",
        
        # 基础限制与展示定义
        max_message_length=4096,
        emoji="🔗",
        allow_update_command=True,
    )
