# -*- coding: utf-8 -*-
"""Business context supplied by the user for AI analysis only."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AnalysisContext(BaseModel):
    """Structured business context bound to an analysis session.

    These fields must never affect retention calculation. They are only used
    when building the LLM context and report metadata.
    """

    game_name: Optional[str] = Field(default=None, description="Game name")
    gameplay: Optional[str] = Field(default=None, description="Short gameplay description")
    game_genre: Optional[str] = Field(default=None, description="Game genre, such as SLG/MMO/Idle")
    recent_events: List[str] = Field(default_factory=list, description="Recent operation or product events")
    main_concern: Optional[str] = Field(default=None, description="Current business concern")
    extra: Dict[str, Any] = Field(default_factory=dict, description="Forward-compatible context fields")

    def compact(self) -> Dict[str, Any]:
        """Return a payload-friendly dict without empty values."""
        data = self.model_dump()
        return {
            key: value
            for key, value in data.items()
            if value not in (None, "", [], {})
        }
