"""Unit tests for the hand-rolled BackupFrame protobuf decoder."""

from __future__ import annotations

import struct

from chatvault.sources.signal.proto import (
    _parse_fields,
    _read_varint,
    decode_backup_frame,
)


def _enc_varint(v: int) -> bytes:
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _enc_tag(field_num: int, wire: int) -> bytes:
    return _enc_varint((field_num << 3) | wire)


def _enc_str(field_num: int, s: str) -> bytes:
    raw = s.encode("utf-8")
    return _enc_tag(field_num, 2) + _enc_varint(len(raw)) + raw


def _enc_int(field_num: int, v: int) -> bytes:
    return _enc_tag(field_num, 0) + _enc_varint(v)


def _enc_bytes(field_num: int, raw: bytes) -> bytes:
    return _enc_tag(field_num, 2) + _enc_varint(len(raw)) + raw


def _enc_msg(field_num: int, inner: bytes) -> bytes:
    return _enc_tag(field_num, 2) + _enc_varint(len(inner)) + inner


def test_read_varint_single_byte() -> None:
    v, pos = _read_varint(b"\x05", 0)
    assert v == 5
    assert pos == 1


def test_read_varint_multibyte() -> None:
    # 300 = 0b10010 0101100 → little-endian groups → 0xAC 0x02
    v, pos = _read_varint(b"\xac\x02", 0)
    assert v == 300
    assert pos == 2


def test_parse_fields_mixed_wires() -> None:
    buf = _enc_str(1, "hi") + _enc_int(2, 42) + _enc_bytes(3, b"\xff\x00\x10")
    fields = _parse_fields(buf)
    assert fields[1][0] == (2, b"hi")
    assert fields[2][0] == (0, 42)
    assert fields[3][0] == (2, b"\xff\x00\x10")


def test_decode_backup_frame_header() -> None:
    header_inner = _enc_bytes(1, b"\x01" * 16) + _enc_bytes(2, b"\x02" * 16) + _enc_int(3, 1)
    raw = _enc_msg(1, header_inner)  # field 1 of BackupFrame = Header
    frame = decode_backup_frame(raw)
    assert frame.header is not None
    assert frame.header.iv == b"\x01" * 16
    assert frame.header.salt == b"\x02" * 16
    assert frame.header.version == 1


def test_decode_backup_frame_sql_statement_with_parameters() -> None:
    # SqlParameter (stringParamter=1, integerParameter=2, nullparameter=5)
    p_string = _enc_msg(2, _enc_str(1, "hello"))
    p_int = _enc_msg(2, _enc_int(2, 7))
    p_null = _enc_msg(2, _enc_int(5, 1))
    stmt_inner = _enc_str(1, "INSERT INTO t VALUES(?, ?, ?)") + p_string + p_int + p_null
    raw = _enc_msg(2, stmt_inner)  # field 2 of BackupFrame = SqlStatement
    frame = decode_backup_frame(raw)
    assert frame.statement is not None
    assert frame.statement.statement == "INSERT INTO t VALUES(?, ?, ?)"
    assert len(frame.statement.parameters) == 3
    assert frame.statement.parameters[0].string_value == "hello"
    assert frame.statement.parameters[1].int_value == 7
    assert frame.statement.parameters[2].is_null is True


def test_decode_backup_frame_attachment_and_end() -> None:
    att_inner = _enc_int(1, 11) + _enc_int(2, 22) + _enc_int(3, 333)
    raw = _enc_msg(4, att_inner)
    frame = decode_backup_frame(raw)
    assert frame.attachment is not None
    assert frame.attachment.row_id == 11
    assert frame.attachment.attachment_id == 22
    assert frame.attachment.length == 333

    frame_end = decode_backup_frame(_enc_int(6, 1))
    assert frame_end.end is True


def test_decode_unknown_fields_are_ignored() -> None:
    # Unknown field 99 shouldn't break decoding.
    raw = _enc_msg(1, _enc_bytes(1, b"\x00" * 16) + _enc_bytes(2, b"\x00" * 16)) + _enc_int(99, 42)
    frame = decode_backup_frame(raw)
    assert frame.header is not None


def test_big_endian_lengths_in_outer_protocol() -> None:
    # Sanity check: the >I packing used by the decrypt module produces network-order ints
    assert struct.pack(">I", 0x01020304) == b"\x01\x02\x03\x04"
