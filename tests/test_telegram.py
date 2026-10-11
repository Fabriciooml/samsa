import httpx
import pytest

from telegram_kafka.telegram import PermanentSendError, TelegramSender


@pytest.mark.asyncio
async def test_sender_uses_bot_token_chat_and_markdownv2():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        sender = TelegramSender(client, base_url="https://api.telegram.org", chat_interval=0)
        await sender.send("123:SECRET", -100123, "*Order:* A\\_1")

    assert requests[0].url.path == "/bot123:SECRET/sendMessage"
    assert requests[0].read().decode().count("MarkdownV2") == 1
    assert '"chat_id":-100123' in requests[0].read().decode()


@pytest.mark.asyncio
async def test_sender_honors_retry_after_then_succeeds():
    calls = 0
    waits = []

    def respond(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429, json={"ok": False, "parameters": {"retry_after": 3}}
            )
        return httpx.Response(200, json={"ok": True})

    async def sleep(seconds):
        waits.append(seconds)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        sender = TelegramSender(client, chat_interval=0, sleep=sleep)
        await sender.send("123:SECRET", -100123, "hello")

    assert calls == 2
    assert waits == [3]


@pytest.mark.asyncio
async def test_sender_reports_permanent_telegram_rejection_without_token():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(403, json={"ok": False}))
    ) as client:
        sender = TelegramSender(client, chat_interval=0)
        with pytest.raises(PermanentSendError) as exc:
            await sender.send("123:SECRET", -100123, "hello")
    assert "SECRET" not in str(exc.value)
