"""Router for analytics endpoints."""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog as Interaction
from app.models.item import ItemRecord as Item
from app.models.learner import Learner

router = APIRouter()


async def require_api_key(
    authorization: str | None = Header(default=None),
) -> str:
    expected_api_key = os.getenv("API_KEY")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authorization header",
        )

    provided_api_key = authorization.removeprefix("Bearer ").strip()

    if expected_api_key and provided_api_key != expected_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    return provided_api_key


async def _get_lab_task_ids(session: AsyncSession, lab: str) -> list[int]:
    lab_title = lab.replace("lab-", "Lab ")

    lab_item = (
        await session.exec(
            select(Item).where(Item.title.contains(lab_title))
        )
    ).first()

    if lab_item is None:
        return []

    task_ids = (
        await session.exec(
            select(Item.id).where(Item.parent_id == lab_item.id)
        )
    ).all()

    return list(task_ids)


@router.get("/scores", dependencies=[Depends(require_api_key)])
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_lab_task_ids(session, lab)

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    stmt = (
        select(
            case(
                (Interaction.score <= 25, "0-25"),
                (Interaction.score <= 50, "26-50"),
                (Interaction.score <= 75, "51-75"),
                else_="76-100",
            ).label("bucket"),
            func.count().label("count"),
        )
        .where(
            Interaction.item_id.in_(task_ids),
            Interaction.score.is_not(None),
        )
        .group_by("bucket")
    )

    rows = (await session.exec(stmt)).all()

    buckets = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0,
    }

    for bucket, count in rows:
        buckets[bucket] = count

    return [{"bucket": bucket, "count": count} for bucket, count in buckets.items()]


@router.get("/pass-rates", dependencies=[Depends(require_api_key)])
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_lab_task_ids(session, lab)

    if not task_ids:
        return []

    stmt = (
        select(
            Item.title,
            func.round(func.avg(Interaction.score), 1).label("avg_score"),
            func.count().label("attempts"),
        )
        .join(Interaction, Interaction.item_id == Item.id)
        .where(
            Item.id.in_(task_ids),
            Interaction.score.is_not(None),
        )
        .group_by(Item.title)
        .order_by(Item.title)
    )

    rows = (await session.exec(stmt)).all()

    return [
        {
            "task": title,
            "avg_score": avg_score,
            "attempts": attempts,
        }
        for title, avg_score, attempts in rows
    ]


@router.get("/timeline", dependencies=[Depends(require_api_key)])
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_lab_task_ids(session, lab)

    if not task_ids:
        return []

    stmt = (
        select(
            func.date(Interaction.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(Interaction.item_id.in_(task_ids))
        .group_by("date")
        .order_by("date")
    )

    rows = (await session.exec(stmt)).all()

    return [
        {"date": str(date), "submissions": submissions}
        for date, submissions in rows
    ]


@router.get("/groups", dependencies=[Depends(require_api_key)])
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    task_ids = await _get_lab_task_ids(session, lab)

    if not task_ids:
        return []

    stmt = (
        select(
            Learner.student_group,
            func.round(func.avg(Interaction.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(Interaction, Interaction.learner_id == Learner.id)
        .where(
            Interaction.item_id.in_(task_ids),
            Interaction.score.is_not(None),
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    rows = (await session.exec(stmt)).all()

    return [
        {
            "group": group,
            "avg_score": avg_score,
            "students": students,
        }
        for group, avg_score, students in rows
    ]