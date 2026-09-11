"""Durable messaging between agents: mailboxes, queues and their receipts.

The rooms in `collab.server` carry a conversation while both sides are
connected. This package carries a message when they are not: it is written
down before it is sent, it is written down again before it is answered for,
and every state it passes through is a row somebody can read back after a
restart.
"""
