from __future__ import annotations
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

_PLACEHOLDER_NULLS = {"N/D", "#N/A"}
_PLACEHOLDER_CATEG = {"-", "..", "?"}

def _nullify_placeholders(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip()
    if v in _PLACEHOLDER_NULLS:
        return None
    if v in _PLACEHOLDER_CATEG:
        return None
    return v

class Track(BaseModel):
    track_id: str = Field(..., description="ID único da faixa (ex: R0001)")
    artist: str = Field(..., min_length=1, description="Artista ou banda")
    country: Optional[str] = Field(None, description="País de origem")
    subgenre: Optional[str] = Field(None, description="Subgênero (ex: Grunge, Indie Rock)")
    album: Optional[str] = Field(None, description="Álbum")
    track_title: str = Field(..., min_length=1, description="Título da faixa")
    release_year: int = Field(..., ge=1950, le=datetime.now().year, description="Ano de lançamento")
    duration_sec: int = Field(..., gt=0, description="Duração em segundos")
    tempo_bpm: Optional[int] = Field(None, ge=30, le=260, description="Tempo estimado em BPM")
    key: Optional[str] = Field(None, description="Tom/harmonia (ex: C#m)")
    mode: Optional[str] = Field(None, description="Modo (ex: major, minor)")
    popularity: Optional[int] = Field(None, ge=0, le=100, description="Popularidade (0-100)")
    danceability: Optional[int] = Field(None, ge=0, le=100, description="Dançabilidade (0-100)")
    energy: Optional[int] = Field(None, ge=0, le=100, description="Energia (0-100)")
    loudness_db: Optional[float] = Field(None, description="Loudness em dB")
    label: Optional[str] = Field(None, description="Gravadora")
    language: Optional[str] = Field(None, description="Idioma predominante (ex: English, Spanish)")
    explicit: Optional[bool] = Field(None, description="Possui conteúdo explícito")
    live_recording: Optional[bool] = Field(None, description="Gravação ao vivo")
    notes: Optional[str] = Field(None, description="Observações")

    @validator("artist", "country", "subgenre", "album", "track_title", "key", "mode", "label", "language", "notes", pre=True, always=True)
    def _clean_str(cls, v):
        v = _nullify_placeholders(v)
        return v if v is None else str(v).strip()

    @validator("release_year")
    def _valid_year(cls, v):
        now_year = datetime.now().year
        if v < 1950 or v > now_year:
            raise ValueError(f"Ano inválido: {v}")
        return v

    @property
    def duration_min(self) -> float:
        return round(self.duration_sec / 60.0, 3)

    @property
    def decade(self) -> int:
        return (self.release_year // 10) * 10

    @property
    def is_english(self) -> Optional[bool]:
        if self.language is None:
            return None
        return self.language.strip().lower() == "english"

    @property
    def is_spanish(self) -> Optional[bool]:
        if self.language is None:
            return None
        return self.language.strip().lower() == "spanish"

    @property
    def tempo_bucket(self) -> Optional[str]:
        if self.tempo_bpm is None:
            return None
        bpm = self.tempo_bpm
        if bpm < 90:
            return "slow"
        if bpm <= 130:
            return "medium"
        return "fast"

    @property
    def energy_bucket(self) -> Optional[str]:
        if self.energy is None:
            return None
        e = self.energy
        if e < 34:
            return "low"
        if e < 67:
            return "mid"
        return "high"

    @property
    def label_group(self) -> Optional[str]:
        if not self.label:
            return None
        l = self.label.strip().lower()
        if any(k in l for k in ["sony", "columbia", "rca", "epic"]):
            return "Sony"
        if any(k in l for k in ["universal", "umg", "island", "def jam", "interscope"]):
            return "Universal"
        if any(k in l for k in ["warner", "atlantic", "elektra", "asylum"]):
            return "Warner"
        return "Independent/Other"

    @property
    def region(self) -> Optional[str]:
        if not self.country:
            return None
        c = self.country.strip().lower()
        americas = {"united states", "canada", "mexico", "brazil", "argentina", "chile", "colombia"}
        europe = {"united kingdom", "finland", "germany", "france", "italy", "spain", "portugal", "sweden", "norway", "denmark", "netherlands", "belgium", "ireland"}
        if c in americas:
            return "Americas"
        if c in europe:
            return "Europe"
        return "Other"

class Envelope(BaseModel):
    """
    Estrutura esperada dentro do JSON bruto em Bronze:
    {
      "track_id": "...",
      "artist": "...",
      ...
    }
    """
    record: Track

    @validator("record", pre=True)
    def _accept_flat_or_nested(cls, v):
        return v if isinstance(v, dict) and "track_id" not in v else v
