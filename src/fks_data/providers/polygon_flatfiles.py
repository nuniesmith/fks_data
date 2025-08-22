"""Polygon Flat Files (S3) helper functions."""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import os, gzip, io

BucketName = "flatfiles"


def list_objects(s3_client, prefix: str, max_items: int) -> Dict[str, Any]:
    if not s3_client:
        return {"ok": False, "error": "missing s3 client"}
    paginator = s3_client.get_paginator("list_objects_v2")
    items: List[Dict[str, Any]] = []
    for page in paginator.paginate(Bucket=BucketName, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            items.append({
                "key": obj.get("Key"),
                "size": int(obj.get("Size", 0)),
                "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                "storage_class": obj.get("StorageClass"),
            })
            if max_items and len(items) >= max_items:
                break
        if max_items and len(items) >= max_items:
            break
    return {"ok": True, "items": items, "count": len(items)}


def sample_object(s3_client, key: str, n: int, include_header: bool) -> Dict[str, Any]:
    if not s3_client:
        return {"ok": False, "error": "missing s3 client"}
    obj = s3_client.get_object(Bucket=BucketName, Key=key)
    body = obj["Body"].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
            text = gz.read().decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        text = body.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not include_header and lines:
        lines = lines[1:]
    out_lines = lines[: max(1, n)]
    return {"ok": True, "key": key, "lines": out_lines, "returned": len(out_lines)}


def download_object(s3_client, key: str, dest_abs: str) -> Tuple[bool, Dict[str, Any]]:
    if not s3_client:
        return False, {"ok": False, "error": "missing s3 client"}
    os.makedirs(os.path.dirname(dest_abs), exist_ok=True)
    s3_client.download_file(BucketName, key, dest_abs)
    size = os.path.getsize(dest_abs) if os.path.exists(dest_abs) else None
    return True, {"ok": True, "key": key, "path": dest_abs, "size": size}
