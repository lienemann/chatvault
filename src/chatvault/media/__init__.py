"""Media layout helpers — chat-slug routing, orphan bucket, re-home logic.

`layout.chat_slug(jid, name)` produces the per-chat folder name. The slug is
**stable**: once a chat's first message lands and the folder is created, the
slug is never rebuilt — even if the chat is renamed in the source app. That
keeps every `mirrored_path` in the DB and every existing file path durable.
"""
