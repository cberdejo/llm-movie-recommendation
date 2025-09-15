from typing import Literal
from pydantic import BaseModel, Field


class MediaItem(BaseModel):
    title: str = Field(..., description="Título de la obra audiovisual")
    director: str | None = Field(
        None, description="Director/a principal (si se conoce)"
    )
    cast: list[str] = Field(
        default_factory=list, description="Lista de actores/actrices"
    )
    genre: list[str] = Field(default_factory=list, description="Lista de géneros")
    description: str | None = Field(None, description="Sinopsis o descripción breve")
    duration_min: int | None = Field(None, description="Duración en minutos, si aplica")
    type: Literal["Movie", "TV Show"] = Field(..., description="Tipo de obra")

    class Config:
        extra = "ignore"

    def duration_category(self) -> str | None:
        """Clasify movie by duration."""
        if self.duration_min is None:
            return None
        if self.duration_min <= 90:
            return "short"
        elif self.duration_min <= 120:
            return "Medium"
        else:
            return "Long"

    def __str__(self):
        parts = [f"Título: {self.title}."]
        if self.director:
            parts.append(f"Director: {self.director}.")
        if self.cast:
            parts.append(f"Cast: {', '.join(self.cast)}.")
        if self.genre:
            parts.append(f"Genre: {', '.join(self.genre)}.")
        if self.description:
            parts.append(f"Sinopsis: {self.description}")
        if self.duration_category():
            parts.append(f"Duration: {self.duration_category()}.")
        return " ".join(parts).strip()
