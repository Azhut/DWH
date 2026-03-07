"""Step: sync parsed metadata into acquired file record."""

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError


class SyncFileMetadataStep:
    """Copy metadata from context into file_model."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_model:
            raise CriticalUploadError(
                message="file_model is not set before metadata sync",
                domain="upload.sync_file_metadata",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )

        if not ctx.file_info:
            raise CriticalUploadError(
                message="file_info is not set before metadata sync",
                domain="upload.sync_file_metadata",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )

        ctx.file_model.year = ctx.file_info.year
        ctx.file_model.reporter = ctx.file_info.reporter
