from __future__ import annotations
import asyncio, math, random, re
from typing import Any
from uuid import uuid4
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Plain, Reply
from astrbot.api.star import Context, Star

_PENDING = "interrupt_segmented_pending_id"
_REGEX = r'.*?[。？！~…]+["”’」』】》〉＞›）］｝〕〗〙]*|.+$'
_CLOSERS = set('"”’」』】》〉＞›）］｝〕〗〙')

class InterruptSegmentedReplyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.config = config if config is not None else {}
        self.pending = {}
        self.tasks = {}
        self.senders = {}
        self.interrupted = set()

    @filter.on_decorating_result()
    async def on_decorating_result(self, event: AstrMessageEvent) -> None:
        if not self.get("enabled", True):
            logger.debug("分段回复插件未启用，跳过")
            return
        result = event.get_result()
        if result is None or not result.chain:
            logger.debug("无回复内容，跳过分段")
            return
        if self.get("only_llm_result", True):
            m = getattr(result, "is_model_result", None)
            if callable(m):
                try:
                    if not m():
                        logger.debug("非模型结果，跳过分段")
                        return
                except Exception:
                    pass
        threshold = self.get_int("words_count_threshold", 150)
        new_chain = []
        for comp in result.chain:
            if not isinstance(comp, Plain):
                new_chain.append(comp)
                continue
            text = self.remove_empty_brackets(comp.text)
            if not text:
                continue
            if len(text) > threshold:
                logger.debug("单段文本超过阈值(%d>%d)，该段不分段", len(text), threshold)
                new_chain.append(Plain(text))
                continue
            for s in self.split(text):
                s = self.cleanup(s)
                if s:
                    new_chain.append(Plain(s))
        if len(new_chain) <= 1:
            logger.debug("分段后仅%d段，无需分段发送", len(new_chain))
            return
        header = [c for c in new_chain if isinstance(c, (Reply, At))]
        rest = [c for c in new_chain if not isinstance(c, (Reply, At))]
        if not rest:
            logger.debug("无有效文本段，跳过")
            return
        result.chain = [*header, rest[0]]
        pid = uuid4().hex
        self.pending[pid] = {"session": event.unified_msg_origin, "sender": self.sender(event), "comps": rest[1:]}
        event.set_extra(_PENDING, pid)
        logger.info("分段回复：共%d段，剩余%d段待发送 (session=%s)", len(new_chain), len(rest) - 1, event.unified_msg_origin)

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent) -> None:
        pid = str(event.get_extra(_PENDING, "") or "").strip()
        if not pid:
            return
        p = self.pending.pop(pid, None)
        if not p or not p["comps"]:
            logger.debug("after_message_sent: 无待发送分段 (pid=%s)", pid)
            return
        task = asyncio.create_task(self.send_rest(p["session"], p["sender"], p["comps"]))
        self.tasks[p["session"]] = task
        self.senders[p["session"]] = p["sender"]
        task.add_done_callback(lambda t, s=p["session"]: self.tasks.pop(s, None))
        logger.info("已创建剩余%d段的发送任务 (session=%s)", len(p["comps"]), p["session"])

    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_user_message(self, event: AstrMessageEvent) -> None:
        if not self.get("enabled", True) or not self.get("interrupt_enabled", True):
            return
        msg_obj = getattr(event, "message_obj", None)
        if msg_obj is None:
            return
        mt = str(getattr(msg_obj, "type", ""))
        if mt and "MESSAGE" not in mt.upper():
            return
        if not getattr(msg_obj, "message", None) and not getattr(event, "message_str", ""):
            logger.debug("收到空消息，不视为打断 (session=%s)", event.unified_msg_origin)
            return
        session = event.unified_msg_origin
        task = self.tasks.get(session)
        if task is None:
            logger.debug("收到用户消息但该会话无发送任务，不打断 (session=%s, msg=%r)",
                         session, str(getattr(event, "message_str", ""))[:20])
            return
        if task.done():
            logger.debug("收到用户消息但发送任务已完成，不打断 (session=%s)", session)
            return
        if session in self.interrupted:
            logger.debug("该会话已处于打断状态 (session=%s)", session)
            return
        self.interrupted.add(session)
        logger.info(">>> 检测到打断！停止继续发送剩余分段 (session=%s, msg=%r)",
                    session, str(getattr(event, "message_str", ""))[:20])
        if self.get("notify_interrupted", True):
            notice = str(self.get("interrupt_notice", "已打断～"))
            sent = False
            try:
                await event.send(MessageChain([Plain(notice)]))
                sent = True
            except Exception as e:
                logger.debug("通过 event.send 发送打断提示失败: %s", e)
            if not sent:
                try:
                    await self.context.send_message(session, MessageChain([Plain(notice)]))
                except Exception as e:
                    logger.debug("通过 context.send_message 发送打断提示失败: %s", e)
            logger.info("打断提示已发送: %s", notice)

    async def send_rest(self, session, sender, comps):
        truncated = False
        sent_count = 0
        total = len(comps)
        logger.info("开始发送剩余%d段 (session=%s)", total, session)
        try:
            for comp in comps:
                if session in self.interrupted:
                    truncated = True
                    break
                delay = self.delay(comp)
                if delay > 0:
                    await asyncio.sleep(delay)
                if session in self.interrupted:
                    truncated = True
                    break
                ok = await self.context.send_message(session, MessageChain([comp]))
                sent_count += 1
                logger.info("已发送第%d/%d段 (session=%s)", sent_count, total, session)
                if not ok:
                    logger.warning("发送第%d段失败，停止 (session=%s)", sent_count, session)
                    return
        finally:
            self.interrupted.discard(session)
            self.senders.pop(session, None)
            if truncated:
                logger.info(">>> 分段发送被打断：已发%d段，剩余%d段未发送 (session=%s)",
                            sent_count, total - sent_count, session)
                if self.get("notify_model", True):
                    ok_inject = await self.inject_interrupt(session)
                    logger.info("注入打断标记结果: %s (session=%s)", ok_inject, session)

    async def inject_interrupt(self, session) -> bool:
        base = str(self.get("interrupt_model_mark", "（此条回复被用户打断了，未能完整发送。被打断的部分仍完整保存在上下文中，如需继续请以此为据。）"))
        try:
            # 1) 旧版 API：Context.get_conversation
            try:
                conv = self.context.get_conversation(session)
                if conv is not None:
                    if await self._inject_marker(conv, base):
                        return True
            except Exception as e:
                logger.debug("Context.get_conversation 不可用: %s", e)
            # 2) v4：bot.session_manager / bot.conversation_manager / context.session_manager 等
            bot = getattr(self.context, "bot", None)
            mgr = None
            if bot is not None:
                mgr = getattr(bot, "session_manager", None) or getattr(bot, "conversation_manager", None)
            if mgr is None:
                mgr = getattr(self.context, "session_manager", None) or getattr(self.context, "conversation_manager", None)
            if mgr is not None:
                for getter in ("get_session", "get_conversation"):
                    fn = getattr(mgr, getter, None)
                    if not callable(fn):
                        continue
                    try:
                        conv = fn(session)
                        if asyncio.iscoroutine(conv):
                            conv = await conv
                        if conv is not None and await self._inject_marker(conv, base):
                            return True
                    except Exception as e:
                        logger.debug("通过 %s.%s 获取会话失败: %s", type(mgr).__name__, getter, e)
                try:
                    sessions_map = getattr(mgr, "sessions", None)
                    if isinstance(sessions_map, dict) and session in sessions_map:
                        if await self._inject_marker(sessions_map[session], base):
                            return True
                except Exception as e:
                    logger.debug("通过 sessions 字典获取会话失败: %s", e)
            # 3) platform_mediator 兜底
            pm = getattr(self.context, "platform_mediator", None)
            if pm is not None:
                fn = getattr(pm, "get_conversation", None)
                if callable(fn):
                    try:
                        conv = fn(session)
                        if asyncio.iscoroutine(conv):
                            conv = await conv
                        if conv is not None and await self._inject_marker(conv, base):
                            return True
                    except Exception as e:
                        logger.debug("通过 platform_mediator.get_conversation 获取会话失败: %s", e)
            logger.warning("未能注入打断标记：未找到可用的会话接口 (session=%s)", session)
            return False
        except Exception as e:
            logger.error("注入打断标记异常: %s", e, exc_info=True)
            return False

    async def _inject_marker(self, conv, base) -> bool:
        try:
            # 方式 A：conv.append_message(role, chain)
            am = getattr(conv, "append_message", None)
            if callable(am):
                try:
                    r = am("assistant", MessageChain([Plain(base)]))
                    if asyncio.iscoroutine(r):
                        await r
                    logger.info("已向会话注入打断标记 (append_message): %s", getattr(conv, "session_id", ""))
                    return True
                except Exception as e:
                    logger.debug("append_message(role, chain) 失败，尝试其他方式: %s", e)
                try:
                    msg = self._make_bot_message(conv, base)
                    if msg is not None:
                        r = am(msg)
                        if asyncio.iscoroutine(r):
                            await r
                        logger.info("已向会话注入打断标记 (append_message msg): %s", getattr(conv, "session_id", ""))
                        return True
                except Exception as e:
                    logger.debug("append_message(msg) 失败: %s", e)
            # 方式 B：conv.update_message 追加到最后一条消息
            msgs = None
            gm = getattr(conv, "get_messages", None)
            if callable(gm):
                try:
                    msgs = gm()
                    if asyncio.iscoroutine(msgs):
                        msgs = await msgs
                except Exception:
                    msgs = None
            if msgs is None:
                msgs = getattr(conv, "messages", None)
            if msgs:
                last = msgs[-1]
                msg_id = getattr(last, "id", None) or getattr(last, "message_id", None)
                um = getattr(conv, "update_message", None)
                if msg_id is not None and callable(um):
                    old_chain = list(getattr(last, "chain", []) or [])
                    old_chain.append(Plain(base))
                    r = um(msg_id, MessageChain(old_chain), "assistant")
                    if asyncio.iscoroutine(r):
                        await r
                    logger.info("已向会话注入打断标记 (update_message): %s", getattr(conv, "session_id", ""))
                    return True
        except Exception as e:
            logger.debug("注入打断标记失败: %s", e, exc_info=True)
        return False

    def _make_bot_message(self, conv, base):
        try:
            from astrbot.core.platform.astrbot_message import AstrBotMessage
        except Exception:
            try:
                from astrbot.core.astrbot_message import AstrBotMessage
            except Exception:
                logger.debug("无法导入 AstrBotMessage")
                return None
        try:
            import time
            sid = getattr(conv, "session_id", None) or getattr(conv, "id", None) or ""
            return AstrBotMessage(
                session_id=sid,
                sender_id="system",
                sender_name="AstrBot",
                message=MessageChain([Plain(base)]),
                message_str=base,
                type="message",
                timestamp=time.time(),
                self_id=getattr(conv, "self_id", "") or "",
                unified_msg_origin=sid,
                platform=getattr(conv, "platform_name", None) or getattr(conv, "platform", "") or "",
            )
        except Exception as e:
            logger.debug("构造 AstrBotMessage 失败: %s", e)
            return None

    def remove_empty_brackets(self, text):
        if not self.get("remove_empty_brackets", True):
            return text
        pairs_raw = str(self.get("empty_brackets", "《》,[],『』,［］,（）,【】,「」"))
        pairs = []
        for pair in pairs_raw.split(","):
            pair = pair.strip()
            if len(pair) == 2:
                pairs.append(pair)
        if not pairs:
            return text
        pattern = re.compile("|".join(re.escape(l) + r"\s*" + re.escape(r) for l, r in pairs))
        for _ in range(5):
            new_text = pattern.sub("", text)
            if new_text == text:
                break
            text = new_text
        return text

    def split(self, text):
        mode = self.get("split_mode", "regex").strip().lower()
        if mode == "words":
            words = [w for w in str(self.get("split_words", "。,？,！,~,…")).split(",") if w]
            if words:
                return self.split_words(text, words)
        regex = str(self.get("regex", _REGEX))
        if not regex:
            regex = _REGEX
        try:
            res = re.findall(regex, text, re.DOTALL | re.MULTILINE)
        except Exception:
            res = re.findall(_REGEX, text, re.DOTALL | re.MULTILINE)
        if res:
            merged = []
            for seg in res:
                if merged and seg.strip() and all(c in _CLOSERS for c in seg.strip()):
                    merged[-1] += seg
                else:
                    merged.append(seg)
            res = merged
        return res if res else [text]

    def split_words(self, text, words):
        OPENERS = set("([{“「『【《〈＜‘‹（〔〖［｛")
        if not words:
            return [text]
        pat = re.compile("|".join(re.escape(w) for w in sorted(words, key=len, reverse=True)))
        parts = []
        last = 0
        pos = 0
        n = len(text)
        while pos < n:
            m = pat.search(text, pos)
            if not m:
                break
            if m.group() in OPENERS:
                pos = m.end()
                continue
            end = m.end()
            while end < n:
                ch = text[end]
                if ch.isalnum() or ('\u4e00' <= ch <= '\u9fff'):
                    break
                if ch in OPENERS:
                    break
                if ch in _CLOSERS and text[end - 1] in '\n\r ':
                    break
                end += 1
            if end > last:
                parts.append(text[last:end])
            last = end
            pos = end
        if last < n:
            parts.append(text[last:])
        return parts if parts else [text]

    def cleanup(self, seg):
        rule = str(self.get("content_cleanup_rule", "") or "")
        if rule:
            try:
                seg = re.sub(rule, "", seg)
            except Exception:
                pass
        return seg.strip()

    def delay(self, comp):
        if str(self.get("interval_method", "random")).strip().lower() == "log":
            if isinstance(comp, Plain):
                try:
                    base = self.get_float("log_base", 2.6)
                    t = len(comp.text) + 1
                    return random.uniform(math.log(t, base), math.log(t, base) + 0.5)
                except Exception:
                    pass
            return random.uniform(1, 1.75)
        try:
            rng = [float(x) for x in str(self.get("interval", "3.5,4.5")).replace(" ", "").split(",")]
            return random.uniform(rng[0], rng[1])
        except Exception:
            return random.uniform(3.5, 4.5)

    def get(self, key, default):
        try:
            return self.config.get(key, default)
        except Exception:
            return default

    def get_int(self, key, default):
        try:
            return int(self.get(key, default))
        except Exception:
            return default

    def get_float(self, key, default):
        try:
            return float(self.get(key, default))
        except Exception:
            return default

    @staticmethod
    def sender(event):
        try:
            return str(event.get_sender_id())
        except Exception:
            return ""

    async def terminate(self):
        for t in list(self.tasks.values()):
            if not t.done():
                t.cancel()
        if self.tasks:
            await asyncio.gather(*list(self.tasks.values()), return_exceptions=True)
        self.tasks.clear()
