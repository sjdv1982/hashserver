import gzip

import zstandard

from compression_utils import compress_bytes, decompress_bytes


def test_compression_utils_round_trip():
    payloads = [b"", b"hello", b"0123456789" * 10000]
    for suffix in (".zst", ".gz"):
        for payload in payloads:
            assert decompress_bytes(compress_bytes(payload, suffix), suffix) == payload


def test_compression_utils_match_standard_codecs():
    payload = b"payload" * 100
    assert (
        decompress_bytes(zstandard.ZstdCompressor().compress(payload), ".zst")
        == payload
    )
    assert decompress_bytes(gzip.compress(payload), ".gz") == payload
