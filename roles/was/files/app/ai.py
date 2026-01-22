# app/ai.py
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db import AsyncSessionLocal


# ======================================================
# 1️⃣ 최종 선택 영상 저장
# ======================================================
async def insert_final_video(
    *,
    video_key: str,
    user_id: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
):
    """
    사용자가 최종 선택한 영상 기록
    - 실제 영상 파일은 MinIO에 이미 존재
    - DB에는 '최종 선택 결과' 메타데이터만 저장
    """

    async with AsyncSessionLocal() as session:
        try:
            await session.execute(
                text("""
                    INSERT INTO ai_final_videos
                    (video_key, user_id, title, description)
                    VALUES (:video_key, :user_id, :title, :description)
                    ON CONFLICT (video_key) DO NOTHING
                """),
                {
                    "video_key": video_key,
                    "user_id": user_id,
                    "title": title,
                    "description": description,
                }
            )
            await session.commit()

        except SQLAlchemyError as e:
            await session.rollback()
            raise RuntimeError(f"[ai] insert_final_video failed: {e}")


# ======================================================
# 2️⃣ YouTube 업로드 완료 처리
# ======================================================
async def mark_youtube_uploaded(
    *,
    video_key: str,
    youtube_video_id: str,
):
    """
    YouTube 업로드 완료 시 상태 업데이트
    """

    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(
                text("""
                    UPDATE ai_final_videos
                    SET
                        youtube_uploaded = TRUE,
                        youtube_video_id = :youtube_video_id,
                        youtube_uploaded_at = now()
                    WHERE video_key = :video_key
                """),
                {
                    "video_key": video_key,
                    "youtube_video_id": youtube_video_id,
                }
            )

            if result.rowcount == 0:
                raise RuntimeError("video_key not found")

            await session.commit()

        except SQLAlchemyError as e:
            await session.rollback()
            raise RuntimeError(f"[ai] mark_youtube_uploaded failed: {e}")


# ======================================================
# 3️⃣ 사용자 라이브러리 조회
# ======================================================
async def get_user_library(user_id: str) -> List[dict]:
    """
    사용자 기준 최종 영상 라이브러리 조회
    (프론트 '내 영상 목록' 용도)
    """

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT
                    video_key,
                    title,
                    description,
                    youtube_uploaded,
                    youtube_video_id,
                    selected_at,
                    youtube_uploaded_at
                FROM ai_final_videos
                WHERE user_id = :user_id
                ORDER BY selected_at DESC
            """),
            {"user_id": user_id},
        )

        rows = result.mappings().all()
        return [dict(row) for row in rows]


# ======================================================
# 4️⃣ 운영 / 정책 로그 기록
# ======================================================
async def insert_operation_log(
    *,
    user_id: Optional[str],
    log_type: str,
    status: str,
    message: str,
    video_key: Optional[str] = None,
):
    """
    운영 / 정책 / 시스템 로그 기록
    - 실패해도 서비스 흐름은 절대 중단하지 않음
    """

    async with AsyncSessionLocal() as session:
        try:
            await session.execute(
                text("""
                    INSERT INTO ai_operation_logs
                    (user_id, log_type, status, video_key, message)
                    VALUES (:user_id, :log_type, :status, :video_key, :message)
                """),
                {
                    "user_id": user_id,
                    "log_type": log_type,
                    "status": status,
                    "video_key": video_key,
                    "message": message,
                }
            )
            await session.commit()

        except SQLAlchemyError as e:
            await session.rollback()
            # ❗ 로그 실패는 절대 서비스 흐름 방해 ❌
            print(f"[WARN] insert_operation_log failed: {e}")
