"""Pydantic models for UDTF generation results.

These models replace dictionary return types with structured, type-safe objects.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field


class UDTFGenerationResult(BaseModel):
    """Result of UDTF code generation.

    Provides structured, type-safe access to generated UDTF files.

    Args:
        generated_files: Dictionary mapping view external_id to generated file path
        output_dir: Directory where files were generated
        total_count: Total number of generated files
    """

    generated_files: dict[str, Path] = Field(default_factory=dict)
    output_dir: Path
    total_count: int = 0

    @property
    def file_paths(self) -> list[Path]:
        """List of all generated file paths.

        Returns:
            List of Path objects for all generated files
        """
        return list(self.generated_files.values())

    def get_file(self, view_id: str) -> Path | None:
        """Get file path for a specific view_id.

        Args:
            view_id: The external_id of the view

        Returns:
            Path to the generated file if found, None otherwise
        """
        return self.generated_files.get(view_id)

    def __getitem__(self, view_id: str) -> Path:
        """Allow dict-like access: result['view_id'].

        Args:
            view_id: The external_id of the view

        Returns:
            Path to the generated file

        Raises:
            KeyError: If view_id is not found
        """
        path = self.get_file(view_id)
        if path is None:
            raise KeyError(f"View ID '{view_id}' not found in generation results")
        return path


class ViewSQLGenerationResult(BaseModel):
    """Result of View SQL generation.

    Provides structured, type-safe access to generated SQL statements.

    Args:
        view_sqls: Dictionary mapping view external_id to SQL CREATE VIEW statement
        total_count: Total number of generated SQL statements
    """

    view_sqls: dict[str, str] = Field(default_factory=dict)
    total_count: int = 0

    def get_sql(self, view_id: str) -> str | None:
        """Get SQL for a specific view_id.

        Args:
            view_id: The external_id of the view

        Returns:
            SQL CREATE VIEW statement if found, None otherwise
        """
        return self.view_sqls.get(view_id)

    def __getitem__(self, view_id: str) -> str:
        """Allow dict-like access: result['view_id'].

        Args:
            view_id: The external_id of the view

        Returns:
            SQL CREATE VIEW statement

        Raises:
            KeyError: If view_id is not found
        """
        sql = self.get_sql(view_id)
        if sql is None:
            raise KeyError(f"View ID '{view_id}' not found in SQL generation results")
        return sql
