
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime

class FloatDataModel(BaseModel):
    client: str = Field(..., alias="Client")
    project: str = Field(..., alias="Project")
    role: str = Field(..., alias="Role")
    name: str = Field(..., alias="Name")
    task: str = Field(..., alias="Task")
    start_date: datetime = Field(..., alias="Start Date")
    end_date: datetime = Field(..., alias="End Date")
    estimated_hours: int = Field(..., alias="Estimated Hours")

    @classmethod
    @field_validator("start_date", "end_date", mode="before")
    def parse_date(cls, value: str) -> datetime:
        """
        Parses the 'Date' field from string to datetime object.
        """
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from e

    @classmethod
    @field_validator("estimated_hours", mode="before")
    def validate_estimated_hours(cls, value):
        """
        Validates the 'Estimated Hours' field to ensure it is an integer.
        """
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            else:
                raise ValueError("Estimated Hours must be an integer")
        elif not isinstance(value, int):
            raise ValueError("Estimated Hours must be an integer")
        return value

    @classmethod
    @field_validator("estimated_hours", mode="before")
    def validate_estimated_hours(cls, value):
        """
        Validates the 'Estimated Hours' field to ensure it is an integer.
        """
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            else:
                raise ValueError("Estimated Hours must be an integer")
        elif not isinstance(value, int):
            raise ValueError("Estimated Hours must be an integer")
        return value

    class Config:
        allow_population_by_field_name = True
        allow_extra = 'ignore'


class ClickUpDataModel(BaseModel):
    client: str = Field(..., alias="Client")
    project: str = Field(..., alias="Project")
    name: str = Field(..., alias="Name")
    task: str = Field(..., alias="Task")
    date: datetime = Field(..., alias="Date")
    hours: float = Field(..., alias="Hours")
    note: Optional[str] = Field(None, alias="Note")
    billable: str = Field(..., alias="Billable")

    @classmethod
    @field_validator("date", mode="before")
    def parse_date(cls, value: str) -> datetime:
        """
        Parses the 'Date' field from string to datetime object.
        """
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format: {value}. Expected YYYY-MM-DD.") from e

    @classmethod
    @field_validator("billable")
    def validate_billable(cls, value: str) -> str:
        """
        Validates the 'Billable' field to ensure it contains either 'Yes' or 'No'.
        """
        if value.strip().lower() == "yes":
            return value
        elif value.strip().lower() == "no":
            return value
        else:
            raise ValueError("Billable field must be 'Yes' or 'No'.")

    @classmethod
    @field_validator("hours", mode="before")
    def validate_hours(cls, value: float) -> float:
        """
        Validates the 'Hours' field to ensure it is a positive float.
        """
        if value < 0:
            raise ValueError("Hours must be a positive number")
        return value

    class Config:
        allow_population_by_field_name = True
        allow_extra = 'ignore'
