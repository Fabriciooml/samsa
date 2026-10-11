# Kafka to Telegram notifications

This service turns JSON messages from one Kafka instance into Telegram messages. A deployment can handle multiple topics.

## Language

**Bot**:
A Telegram bot created through BotFather. Its token grants permission to send messages as that bot.
_Avoid_: Bot unique ID

**Destination chat**:
A Telegram group identified by its chat ID, where a bot sends messages.
_Avoid_: Group name

**Source topic**:
A Kafka topic whose JSON messages may become Telegram messages.
_Avoid_: Queue

**Route**:
A saved rule connecting one source topic, one message template, one bot, and one destination chat. Multiple routes may share a bot or source topic.
_Avoid_: Bot configuration

**Message template**:
The MarkdownV2 text pattern in a route. Placeholders refer to fields in a JSON message from its source topic; inserted values display as literal text.
_Avoid_: Message body

**Delivery**:
One route's attempt to send a rendered message for one Kafka record. A record matching two routes creates two deliveries.
_Avoid_: Event
