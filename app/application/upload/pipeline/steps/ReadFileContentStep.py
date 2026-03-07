import logging

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError

logger = logging.getLogger(__name__)


class ReadFileContentStep:
    """Один раз читает загруженный поток и кэширует байты в контексте."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        try:
            await ctx.file.seek(0)
            content = await ctx.file.read()

            if not content:
                raise CriticalUploadError(
                    message="Uploaded file is empty",
                    domain="upload.read_file",
                    http_status=400,
                    meta={"file_name": ctx.filename},
                )

            ctx.file_content = content
            logger.info("File '%s' loaded into memory: %d bytes", ctx.filename, len(content))

        except CriticalUploadError:
            raise
        except Exception as exc:
            raise CriticalUploadError(
                message=f"Failed to read file '{ctx.filename}': {exc}",
                domain="upload.read_file",
                http_status=500,
                meta={"file_name": ctx.filename, "error": str(exc)},
            ) from exc
