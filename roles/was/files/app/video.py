# app/video.py
import os
import json
import httpx
import subprocess
import tempfile

import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from security import verify_jwt
from minio_client import (
    upload_video,
    upload_thumbnail,
    get_video_stream,
    get_thumbnail_stream,
    list_user_videos,
)

router = APIRouter(tags=["video"])

# ==============================
# KIE
# ==============================
KIE_API_URL = "https://api.kie.ai/api/v1/veo/generate"
KIE_API_KEY = os.getenv("KIE_API_KEY")
if not KIE_API_KEY:
    raise RuntimeError("KIE_API_KEY is not set")

# ==============================
# Redis2 (AI Worker 전용)
# ==============================
REDIS2_HOST = os.getenv("AI_REDIS_HOST", "10.1.1.10")
REDIS2_PORT = int(os.getenv("AI_REDIS_PORT", "6379"))
REDIS2_QUEUE = os.getenv("AI_REDIS_QUEUE", "video_processing_jobs")

redis2 = redis.Redis(
    host=REDIS2_HOST,
    port=REDIS2_PORT,
    decode_responses=True,
)

# ==============================
# 상태 캐시 (UI 조회용)
# ==============================
TASKS = {}

class VideoGenerateRequest(BaseModel):
    prompt: str

# ======================================================
# 1️⃣ 영상 생성 요청
# ======================================================
@router.post("/generate")
async def generate_video(body: VideoGenerateRequest, user=Depends(verify_jwt)):
    user_id = user["sub"]

    payload = {
        "prompt": body.prompt,
        "model": "veo3_fast",
        "aspect_ratio": "9:16",
        "callBackUrl": "https://auth.justic.store/api/video/callback",
    }

    headers = {
        "Authorization": f"Bearer {KIE_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(KIE_API_URL, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()

    task_id = data.get("data", {}).get("taskId")
    if not task_id:
        raise HTTPException(status_code=502, detail="No taskId")

    TASKS[task_id] = {
        "status": "QUEUED",
        "user_id": user_id,
    }

    return {"task_id": task_id, "status": "QUEUED"}

# ======================================================
# 2️⃣ KIE 콜백 → 원본 업로드 → Redis2 PUSH
# ======================================================
@router.post("/callback")
async def video_callback(payload: dict):
    data = payload.get("data", {})
    task_id = data.get("taskId")
    urls = data.get("info", {}).get("resultUrls", [])

    task = TASKS.get(task_id)
    if not task or not urls:
        return {"code": 200}

    user_id = task["user_id"]
    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.get(urls[0])
            r.raise_for_status()
            with open(tmp_video, "wb") as f:
                f.write(r.content)

        upload_video(user_id, task_id, tmp_video)

        job = {
            "task_id": task_id,
            "user_id": user_id,
            "input_key": f"{user_id}/{task_id}.mp4",
            "output_key": f"{user_id}/{task_id}_processed.mp4",
        }

        redis2.lpush(REDIS2_QUEUE, json.dumps(job))
        task["status"] = "QUEUED_FOR_AI"

    except Exception as e:
        task["status"] = "FAILED"
        print("[callback error]", e)

    finally:
        os.remove(tmp_video)

    return {"code": 200}

# ======================================================
# 3️⃣ 영상 목록
# ======================================================
@router.get("/list")
def list_videos(user=Depends(verify_jwt)):
    user_id = user["sub"]
    names = list_user_videos(user_id)

    videos = {}
    for name in names:
        base = name.replace("_processed", "")
        videos.setdefault(base, {
            "task_id": base,
            "has_original": False,
            "has_processed": False,
        })
        if name.endswith("_processed"):
            videos[base]["has_processed"] = True
        else:
            videos[base]["has_original"] = True

    return {"videos": list(videos.values())}

# ======================================================
# 4️⃣ 상태 조회
# ======================================================
@router.get("/status/{task_id}")
def get_status(task_id: str):
    task = TASKS.get(task_id)

    if not task:
        return {"task_id": task_id, "status": "DONE"}

    if task.get("status") == "FAILED":
        return {"task_id": task_id, "status": "FAILED"}

    try:
        user_id = task.get("user_id")
        if user_id:
            names = list_user_videos(user_id)
            if f"{task_id}_processed" in names:
                task["status"] = "DONE"
    except Exception:
        pass

    return {"task_id": task_id, "status": task["status"]}

# ======================================================
# 5️⃣ 영상 스트리밍
# ======================================================
@router.get("/stream/{task_id}")
def stream_video(
    task_id: str,
    type: str = Query("original", enum=["original", "processed"]),
    user=Depends(verify_jwt),
):
    user_id = user["sub"]
    processed = (type == "processed")

    obj = get_video_stream(user_id, task_id, processed)

    def gen():
        for c in obj.stream(1024 * 1024):
            yield c
        obj.close()
        obj.release_conn()

    return StreamingResponse(gen(), media_type="video/mp4")

# ======================================================
# 6️⃣ 썸네일
# ======================================================
@router.get("/thumb/{task_id}.jpg")
def get_thumbnail(task_id: str, user=Depends(verify_jwt)):
    user_id = user["sub"]

    try:
        obj = get_thumbnail_stream(user_id, task_id)
    except Exception:
        tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
        tmp_thumb = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name

        vobj = get_video_stream(user_id, task_id)
        with open(tmp_video, "wb") as f:
            for c in vobj.stream(1024 * 1024):
                f.write(c)

        subprocess.run(
            ["ffmpeg", "-y", "-ss", "00:00:01", "-i", tmp_video, "-frames:v", "1", tmp_thumb],
            check=True,
        )

        upload_thumbnail(user_id, task_id, tmp_thumb)
        obj = get_thumbnail_stream(user_id, task_id)

        os.remove(tmp_video)
        os.remove(tmp_thumb)

    def gen():
        for c in obj.stream(256 * 1024):
            yield c
        obj.close()
        obj.release_conn()

    return StreamingResponse(gen(), media_type="image/jpeg")

# ======================================================
# upload model
# ======================================================
class YouTubeUploadRequest(BaseModel):
    task_id: str
    type: str  # "original" or "processed"
    title: str  # ✅ 추가됨

# ======================================================
# YouTube Upload API
# ======================================================
@router.post("/upload/youtube")
async def upload_youtube(
    body: YouTubeUploadRequest,
    user=Depends(verify_jwt),
):
    print("TITLE:", repr(body.title))  # ✅ 여기

    user_id = user["sub"]
    task_id = body.task_id
    video_type = body.type

    if video_type not in ("original", "processed"):
        raise HTTPException(400, "Invalid video type")

    tmp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name

    try:
        obj = get_video_stream(
            user_id=user_id,
            task_id=task_id,
            processed=(video_type == "processed"),
        )

        with open(tmp_video, "wb") as f:
            for c in obj.stream(1024 * 1024):
                f.write(c)

        obj.close()
        obj.release_conn()

        from googleapiclient.http import MediaFileUpload
        from google_auth import get_youtube_service

        youtube = get_youtube_service(user_id)

        request = youtube.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": body.title,  # ✅ 입력한 제목 사용
                    "description": f"Generated by Justic AI\nTask ID: {task_id}",
                    "categoryId": "22",
                },
                "status": {
                    "privacyStatus": "private",
                },
            },
            media_body=MediaFileUpload(
                tmp_video,
                mimetype="video/mp4",
                resumable=True,
            ),
        )

        response = request.execute()

        return {
            "status": "UPLOADED",
            "youtube_video_id": response.get("id"),
        }
        
    except Exception as e:
        import traceback
        print("🔥 YOUTUBE UPLOAD ERROR 🔥")
        traceback.print_exc()   # ⭐ 핵심: 실제 원인 로그 출력
        raise HTTPException(500, "YouTube upload failed")

    finally:
        if os.path.exists(tmp_video):
            os.remove(tmp_video)

