"""Run the full extractor stack against a synthetic source DB."""

from __future__ import annotations

from chatvault.extractors import (
    bot_messages, calls, chats, communities, edits, group_members, identities,
    messages, newsletter, polls, reactions, status_posts, system_events,
    templates, transcriptions, vcards,
)


def test_full_stack_runs_against_synthetic(populated_source, archive_db) -> None:
    """Smoke test: every extractor in DEFAULT_EXTRACTORS must run without crashing.

    The synthetic source intentionally has only the core tables populated; the
    extended extractors run against empty source tables and should produce zero
    rows, not raise.
    """
    runs = [
        identities, chats, group_members, messages, system_events,
        reactions, edits, calls, polls, status_posts, newsletter,
        transcriptions, vcards, bot_messages, templates, communities,
    ]
    results = []
    for mod in runs:
        # AttributeError here would mean the extractor module is misnamed/missing.
        results.append(mod.extract(populated_source, archive_db))

    by_name = {r.name: r for r in results}
    assert by_name["messages"].rows_written == 3
    assert by_name["reactions"].rows_written == 1
    assert by_name["group_members"].rows_written == 3
    # Extended extractors run cleanly against empty source tables.
    for name in ("calls", "polls", "status_posts", "transcriptions",
                 "vcards", "bot_messages", "templates", "communities"):
        assert by_name[name].rows_written == 0, f"{name} unexpectedly wrote rows"


def test_archive_idempotency_under_double_run(populated_source, archive_db) -> None:
    """Two consecutive full runs must not change the archive after the first."""
    runs = [
        identities.extract, chats.extract, group_members.extract,
        messages.extract, system_events.extract, reactions.extract,
        edits.extract, calls.extract, polls.extract, status_posts.extract,
        newsletter.extract, transcriptions.extract, vcards.extract,
        bot_messages.extract, templates.extract, communities.extract,
    ]
    for fn in runs:
        fn(populated_source, archive_db)
    counts_first = _all_counts(archive_db)
    for fn in runs:
        fn(populated_source, archive_db)
    counts_second = _all_counts(archive_db)
    assert counts_first == counts_second


def _all_counts(conn) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_meta%'"
    ):
        name = r[0]
        out[name] = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    return out
