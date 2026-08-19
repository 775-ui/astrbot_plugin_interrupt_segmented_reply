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
            return
        result = event.get_result()
        if result is None or not result.chain:
            return
        if self.get("only_llm_result", True):
            m = getattr(result, "is_model_result", None)
            if callable(m):
                try:
                    if not m():
                        return
                except Exception:
                    pass
        new_chain = []
        for comp in result.chain:
            if not isinstance(comp, Plain):
                new_chain.append(comp)
                continue
            text = self.remove_empty_brackets(comp.text)
            if not text:
                continue
            if len(text) > self.get_int("words_count_threshold", 150):
                new_chain.append(Plain(text))
                continue
            for s in self.split(text):
                s = self.cleanup(s)
                if s:
                    new_chain.append(Plain(s))
        if len(new_chain) <= 1:
            return
        header = [c for c in new_chain if isinstance(c, (Reply, At))]
        rest = [c for c in new_chain if not isinstance(c, (Reply, At))]
        if not rest:
            return
        result.chain = [*header, rest[0]]
        pid = uuid4().hex
        self.pending[pid] = {"session": event.unified_msg_origin, "sender": self.sender(event), "comps": rest[1:]}
        event.set_extra(_PENDING, pid)
        logger.info("分段回复，剩余%d段", len(rest) - 1)

    @filter.after_message_sent()
    async def after_message_sent(self, event: AstrMessageEvent) -> None:
        pid = str(event.get_extra(_PENDING, "") or "").strip()
        if not pid:
            return
        p = self.pending.pop(pid, None)
        if not p or not p["comps"]:
            return
        task = asyncio.create_task(self.send_rest(p["session"], p["sender"], p["comps"]))
        self.tasks[p["session"]] = task
        self.senders[p["session"]] = p["sender"]
        task.add_done_callback(lambda t, s=p["session"]: self.tasks.pop(s, None))

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
            return
        session = event.unified_msg_origin
        task = self.tasks.get(session)
        if task is None or task.done():
            return
        if session in self.interrupted:
            return
        self.interrupted.add(session)
        if self.get("notify_interrupted", True):
            try:
                await event.send(event.plain_result(str(self.get("interrupt_notice", "已打断～"))))
            except Exception:
                pass

    async def send_rest(self, session, sender, comps):
        truncated = False
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
                if not ok:
                    return
        finally:
            self.interrupted.discard(session)
            self.senders.pop(session, None)
            if truncated and self.get("notify_model", True):
                await self.inject_interrupt(session)

    async def inject_interrupt(self, session):
        try:
            conv = self.context.get_conversation(session)
            if conv is None:
                return
            msgs = conv.get_messages()
            if not msgs:
                return
            base = str(self.get("interrupt_model_mark", "（此条回复被用户打断了，未能完整发送。被打断的部分仍完整保存在上下文中，如需继续请以此为据。）"))
            if hasattr(conv, "append_message"):
                try:
                    conv.append_message("assistant", MessageChain([Plain(base)]))
                    logger.info("已向会话上下文注入打断标记: %s", session)
                    return
                except Exception as e:
                    logger.debug("append_message 注入失败，尝试 update_message: %s", e)
            if hasattr(conv, "update_message") and hasattr(msgs[-1], "chain"):
                msg_id = getattr(msgs[-1], "id", None) or getattr(msgs[-1], "message_id", None)
                if msg_id is not None:
                    old_chain = list(msgs[-1].chain)
                    old_chain.append(Plain(base))
                    conv.update_message(msg_id, MessageChain(old_chain), "assistant")
                    logger.info("已通过 update_message 注入打断标记: %s", session)
        except Exception as e:
            logger.error("注入打断标记失败: %s", e, exc_info=True)

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
