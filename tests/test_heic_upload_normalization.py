"""Regression tests for iOS HEIC uploads mislabeled as JPEG.

Safari/iOS can hand the composer HEIC bytes with a .jpg filename. The upload
boundary must normalize those bytes before native vision sees the attachment.
"""
import io
import json

import api.upload as upload
from api.upload import handle_upload


PNG_1X1 = (
    b'\x89PNG\r\n\x1a\n'
    b'\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde'
    b'\x00\x00\x00\x0bIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N'
    b'\x00\x00\x00\x00IEND\xaeB`\x82'
)


def _fake_heic_bytes():
    return b'\x00\x00\x00$ftypheic\x00\x00\x00\x00mif1MiHEmiafMiHB' + b'payload'


def _multipart_body(fields=None, files=None, boundary=b"heicboundary"):
    fields = fields or {}
    files = files or {}
    body = b""
    for name, value in fields.items():
        body += b"--" + boundary + b"\r\n"
        body += f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        body += str(value).encode() + b"\r\n"
    for name, (filename, data, content_type) in files.items():
        body += b"--" + boundary + b"\r\n"
        body += (
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode()
        body += data + b"\r\n"
    body += b"--" + boundary + b"--\r\n"
    return body, f"multipart/form-data; boundary={boundary.decode()}"


class _FakeHandler:
    def __init__(self, body: bytes, content_type: str):
        self.rfile = io.BytesIO(body)
        self.wfile = io.BytesIO()
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
        }
        self.status = None
        self.sent_headers = {}

    def send_response(self, status):
        self.status = status

    def send_header(self, key, value):
        self.sent_headers[key] = value

    def end_headers(self):
        pass

    def payload(self):
        return json.loads(self.wfile.getvalue().decode("utf-8"))


def test_heic_magic_detected_even_when_filename_is_jpeg():
    assert upload._is_heic_like_image(_fake_heic_bytes())
    assert not upload._is_heic_like_image(b"\xff\xd8\xff\xe0not heic")
    assert not upload._is_heic_like_image(b"prefix ftypheic inside text payload")
    assert not upload._is_heic_like_image(b"\x00\x00\x00\x18ftypmif1\x00\x00\x00\x00mif1")
    assert not upload._is_heic_like_image(b"\x00\x00\x01\x00ftypheic\x00\x00\x00\x00")


def test_heic_conversion_failure_does_not_claim_jpeg_is_usable_image(monkeypatch):
    monkeypatch.setattr(upload, "_convert_heic_upload_to_png", lambda data: None)

    name, data, mime, is_image, converted_from = upload._normalize_upload_image_bytes(
        "image_1234_FBFC.jpg",
        _fake_heic_bytes(),
    )

    assert name == "image_1234_FBFC.jpg"
    assert data == _fake_heic_bytes()
    assert mime == "image/heic"
    assert is_image is False
    assert converted_from is None


def test_heic_conversion_larger_than_upload_cap_falls_back_without_image_flag(monkeypatch):
    monkeypatch.setattr(upload, "MAX_UPLOAD_BYTES", 16)
    monkeypatch.setattr(upload, "_convert_heic_upload_to_png", lambda data: PNG_1X1)

    name, data, mime, is_image, converted_from = upload._normalize_upload_image_bytes(
        "image_1234_FBFC.jpg",
        _fake_heic_bytes(),
    )

    assert name == "image_1234_FBFC.jpg"
    assert data == _fake_heic_bytes()
    assert mime == "image/heic"
    assert is_image is False
    assert converted_from is None


def test_write_upload_bytes_refuses_preexisting_symlink(tmp_path):
    dest = tmp_path / "upload.png"
    target = tmp_path / "outside-target.png"
    dest.symlink_to(target)

    try:
        upload._write_upload_bytes(dest, PNG_1X1)
    except ValueError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("_write_upload_bytes followed or replaced a symlink")

    assert dest.is_symlink()
    assert not target.exists()


def test_upload_converts_mislabeled_heic_jpeg_to_png(tmp_path, monkeypatch):
    attachment_root = tmp_path / "attachments"
    monkeypatch.setenv("HERMES_WEBUI_ATTACHMENT_DIR", str(attachment_root))
    monkeypatch.setattr(upload, "get_session", lambda session_id: object())
    monkeypatch.setattr(upload, "_convert_heic_upload_to_png", lambda data: PNG_1X1)

    body, content_type = _multipart_body(
        fields={"session_id": "sess-ios"},
        files={"file": ("image_1234_FBFC.jpg", _fake_heic_bytes(), "image/jpeg")},
    )
    handler = _FakeHandler(body, content_type)

    handle_upload(handler)

    assert handler.status == 200
    payload = handler.payload()
    assert payload["filename"] == "image_1234_FBFC.png"
    assert payload["mime"] == "image/png"
    assert payload["is_image"] is True
    assert payload["converted_from"] == "image/heic"
    stored = tmp_path / "attachments" / "sess-ios" / "image_1234_FBFC.png"
    assert stored.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert payload["path"] == str(stored)
