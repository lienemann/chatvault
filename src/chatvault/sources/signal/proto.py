"""Minimal hand-rolled protobuf decoder for Signal's BackupFrame.

We don't depend on the ``protobuf`` package — the schema is tiny (one
``BackupFrame`` message with nine optional sub-messages) and the wire format
is straightforward. This keeps chatvault dependency-light: only
``cryptography`` is added for Signal support.

Reference (Open Whisper Systems, GPL-3): the canonical Backups.proto is
embedded at the bottom of this file for traceability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Wire types defined by the protobuf spec.
_W_VARINT = 0
_W_FIXED64 = 1
_W_LEN = 2
_W_FIXED32 = 5


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    val = 0
    shift = 0
    while True:
        if pos >= len(buf):
            msg = "truncated varint"
            raise ValueError(msg)
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7
        if shift >= 64:
            msg = "varint too large"
            raise ValueError(msg)


def _parse_fields(buf: bytes) -> dict[int, list[tuple[int, Any]]]:
    """Return ``{field_number: [(wire_type, raw_value), ...]}``."""
    fields: dict[int, list[tuple[int, Any]]] = {}
    pos = 0
    while pos < len(buf):
        tag, pos = _read_varint(buf, pos)
        field_num = tag >> 3
        wire = tag & 0x7
        if wire == _W_VARINT:
            value, pos = _read_varint(buf, pos)
        elif wire == _W_FIXED64:
            value = buf[pos : pos + 8]
            pos += 8
        elif wire == _W_LEN:
            length, pos = _read_varint(buf, pos)
            value = buf[pos : pos + length]
            pos += length
        elif wire == _W_FIXED32:
            value = buf[pos : pos + 4]
            pos += 4
        else:
            msg = f"unsupported wire type {wire} at pos {pos}"
            raise ValueError(msg)
        fields.setdefault(field_num, []).append((wire, value))
    return fields


def _opt(fields: dict[int, list[tuple[int, Any]]], num: int) -> Any:
    entries = fields.get(num)
    if not entries:
        return None
    _, value = entries[0]
    return value


def _opt_str(fields: dict[int, list[tuple[int, Any]]], num: int) -> str | None:
    v = _opt(fields, num)
    if v is None:
        return None
    return v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v


def _opt_int(fields: dict[int, list[tuple[int, Any]]], num: int) -> int | None:
    v = _opt(fields, num)
    return v if v is None or isinstance(v, int) else None


def _opt_bytes(fields: dict[int, list[tuple[int, Any]]], num: int) -> bytes | None:
    v = _opt(fields, num)
    return v if isinstance(v, (bytes, bytearray)) else None


# ---------------------------------------------------------------------------
# Sub-message dataclasses. Only fields chatvault uses are exposed.
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Header:
    iv: bytes
    salt: bytes
    version: int | None  # 1 = encrypted-length frames; absent/0 = plain length


@dataclass(slots=True)
class SqlParameter:
    string_value: str | None = None
    int_value: int | None = None
    double_value: float | None = None
    blob_value: bytes | None = None
    is_null: bool = False


@dataclass(slots=True)
class SqlStatement:
    statement: str
    parameters: list[SqlParameter] = field(default_factory=list)


@dataclass(slots=True)
class Attachment:
    row_id: int
    attachment_id: int
    length: int


@dataclass(slots=True)
class Avatar:
    name: str | None
    recipient_id: str | None
    length: int


@dataclass(slots=True)
class Sticker:
    row_id: int
    length: int


@dataclass(slots=True)
class DatabaseVersion:
    version: int


@dataclass(slots=True)
class SharedPreference:
    file: str | None
    key: str | None
    value: str | None
    boolean_value: bool | None
    string_set: list[str]
    is_string_set: bool


@dataclass(slots=True)
class KeyValue:
    key: str | None
    string_value: str | None
    blob_value: bytes | None
    boolean_value: bool | None
    integer_value: int | None
    long_value: int | None


@dataclass(slots=True)
class BackupFrame:
    header: Header | None = None
    statement: SqlStatement | None = None
    preference: SharedPreference | None = None
    attachment: Attachment | None = None
    db_version: DatabaseVersion | None = None
    end: bool = False
    avatar: Avatar | None = None
    sticker: Sticker | None = None
    key_value: KeyValue | None = None


# ---------------------------------------------------------------------------
# Decoders
# ---------------------------------------------------------------------------


def _decode_header(buf: bytes) -> Header:
    f = _parse_fields(buf)
    iv = _opt_bytes(f, 1) or b""
    salt = _opt_bytes(f, 2) or b""
    version = _opt_int(f, 3)
    return Header(iv=iv, salt=salt, version=version)


def _decode_sql_parameter(buf: bytes) -> SqlParameter:
    f = _parse_fields(buf)
    return SqlParameter(
        string_value=_opt_str(f, 1),
        int_value=_opt_int(f, 2),
        double_value=None,  # Signal uses doubles only in rare schema rows; left null
        blob_value=_opt_bytes(f, 4),
        is_null=bool(_opt_int(f, 5)),
    )


def _decode_sql_statement(buf: bytes) -> SqlStatement:
    f = _parse_fields(buf)
    stmt = _opt_str(f, 1) or ""
    params: list[SqlParameter] = []
    for wire, raw in f.get(2, []):
        if wire == _W_LEN:
            params.append(_decode_sql_parameter(raw))
    return SqlStatement(statement=stmt, parameters=params)


def _decode_attachment(buf: bytes) -> Attachment:
    f = _parse_fields(buf)
    return Attachment(
        row_id=_opt_int(f, 1) or 0,
        attachment_id=_opt_int(f, 2) or 0,
        length=_opt_int(f, 3) or 0,
    )


def _decode_avatar(buf: bytes) -> Avatar:
    f = _parse_fields(buf)
    return Avatar(
        name=_opt_str(f, 1),
        recipient_id=_opt_str(f, 3),
        length=_opt_int(f, 2) or 0,
    )


def _decode_sticker(buf: bytes) -> Sticker:
    f = _parse_fields(buf)
    return Sticker(row_id=_opt_int(f, 1) or 0, length=_opt_int(f, 2) or 0)


def _decode_db_version(buf: bytes) -> DatabaseVersion:
    f = _parse_fields(buf)
    return DatabaseVersion(version=_opt_int(f, 1) or 0)


def _decode_shared_preference(buf: bytes) -> SharedPreference:
    f = _parse_fields(buf)
    return SharedPreference(
        file=_opt_str(f, 1),
        key=_opt_str(f, 2),
        value=_opt_str(f, 3),
        boolean_value=bool(_opt_int(f, 4)) if 4 in f else None,
        string_set=[
            (raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else "")
            for w, raw in f.get(5, [])
            if w == _W_LEN
        ],
        is_string_set=bool(_opt_int(f, 6)),
    )


def _decode_key_value(buf: bytes) -> KeyValue:
    f = _parse_fields(buf)
    return KeyValue(
        key=_opt_str(f, 1),
        blob_value=_opt_bytes(f, 2),
        boolean_value=bool(_opt_int(f, 3)) if 3 in f else None,
        string_value=_opt_str(f, 7),
        integer_value=_opt_int(f, 5),
        long_value=_opt_int(f, 6),
    )


def decode_backup_frame(buf: bytes) -> BackupFrame:
    """Decode a single BackupFrame from raw protobuf bytes."""
    f = _parse_fields(buf)
    frame = BackupFrame()
    if 1 in f:
        wire, raw = f[1][0]
        if wire == _W_LEN:
            frame.header = _decode_header(raw)
    if 2 in f:
        wire, raw = f[2][0]
        if wire == _W_LEN:
            frame.statement = _decode_sql_statement(raw)
    if 3 in f:
        wire, raw = f[3][0]
        if wire == _W_LEN:
            frame.preference = _decode_shared_preference(raw)
    if 4 in f:
        wire, raw = f[4][0]
        if wire == _W_LEN:
            frame.attachment = _decode_attachment(raw)
    if 5 in f:
        wire, raw = f[5][0]
        if wire == _W_LEN:
            frame.db_version = _decode_db_version(raw)
    if 6 in f:
        frame.end = bool(_opt_int(f, 6))
    if 7 in f:
        wire, raw = f[7][0]
        if wire == _W_LEN:
            frame.avatar = _decode_avatar(raw)
    if 8 in f:
        wire, raw = f[8][0]
        if wire == _W_LEN:
            frame.sticker = _decode_sticker(raw)
    if 9 in f:
        wire, raw = f[9][0]
        if wire == _W_LEN:
            frame.key_value = _decode_key_value(raw)
    return frame


# ---------------------------------------------------------------------------
# Canonical proto, kept for reference / future migration to generated bindings.
# Source: Open Whisper Systems, app/src/main/proto/Backups.proto (historical).
# ---------------------------------------------------------------------------
BACKUPS_PROTO_REFERENCE = """\
message SqlStatement {
    message SqlParameter {
        optional string stringParamter   = 1;
        optional uint64 integerParameter = 2;
        optional double doubleParameter  = 3;
        optional bytes  blobParameter    = 4;
        optional bool   nullparameter    = 5;
    }
    optional string       statement  = 1;
    repeated SqlParameter parameters = 2;
}
message SharedPreference {
    optional string file             = 1;
    optional string key              = 2;
    optional string value            = 3;
    optional bool   booleanValue     = 4;
    repeated string stringSetValue   = 5;
    optional bool   isStringSetValue = 6;
}
message Attachment      { optional uint64 rowId = 1; optional uint64 attachmentId = 2; optional uint32 length = 3; }
message Sticker         { optional uint64 rowId = 1; optional uint32 length = 2; }
message Avatar          { optional string name = 1; optional string recipientId = 3; optional uint32 length = 2; }
message DatabaseVersion { optional uint32 version = 1; }
message Header          { optional bytes iv = 1; optional bytes salt = 2; optional uint32 version = 3; }
message KeyValue        { optional string key = 1; optional bytes blobValue = 2; optional bool booleanValue = 3;
                          optional float floatValue = 4; optional int32 integerValue = 5;
                          optional int64 longValue = 6; optional string stringValue = 7; }
message BackupFrame {
    optional Header           header     = 1;
    optional SqlStatement     statement  = 2;
    optional SharedPreference preference = 3;
    optional Attachment       attachment = 4;
    optional DatabaseVersion  version    = 5;
    optional bool             end        = 6;
    optional Avatar           avatar     = 7;
    optional Sticker          sticker    = 8;
    optional KeyValue         keyValue   = 9;
}
"""
