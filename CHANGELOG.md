# Changelog

[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), SemVer.

## [Unreleased]

- Refuse to open an archive whose `schema_version` is newer than the
  running build, instead of silently querying columns that may have
  moved (`SchemaTooNewError`, exit code 2).

Initial public release. SQLite schema (`migrations/001_init.sql`),
incremental extract pipeline, identity resolver with vCard import, the
usual queries (digest, search, timeline, links, forgotten, members,
chat info, chat why), Markdown export, optional inotify-based media
mirror.

[Unreleased]: https://github.com/lienemann/chatvault/commits/main
