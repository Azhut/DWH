from typing import List, Tuple, Optional

from fastapi import HTTPException, UploadFile
from app.core.exceptions import log_and_raise_http
from app.core.logger import logger
from app.data.repositories import FileRepository
from app.parsers.parsers import get_sheet_parser
from app.services.sheet_extraction_service import SheetExtractionService
from app.models.sheet_model import SheetModel
from app.models.file_status import FileStatus
from app.core.database import mongo_connection
from app.models.file_model import FileModel as DomainFileModel

class SheetProcessor:
    def __init__(self):
        self.sheet_extractor = SheetExtractionService()

    async def extract_and_process_sheets(
        self,
        file: UploadFile,
        file_model: DomainFileModel,
        form_id: str,
        skip_sheets: Optional[List[int]] = None,
        spravochno_keywords: Optional[List[str]] = None
    ) -> Tuple[List[SheetModel], List[dict]]:
        """
        file_model: объект FileModel с уже сгенерированным file_id (UUID).
        Возвращает (sheet_models, flat_data) — flat_data уже содержит поле 'file_id' и 'form'
        """
        try:
            # Проверка дублей по filename
            if not await is_file_unique(file.filename):
                msg = f"Файл '{file.filename}' уже был загружен."
                raise HTTPException(status_code=400, detail=msg)

            sheets = await self.sheet_extractor.extract(file)
            sheet_models, flat_data = await self._process_sheets(
                file_model.file_id,
                sheets,
                file_model.city,
                file_model.year,
                form_id,
                skip_sheets,
                spravochno_keywords
            )
            logger.info(f"Успешно обработано {len(sheets)} листов файла {file.filename}")

            # Проставляем file_id в каждой flat записи (если ещё не проставлен)
            for rec in flat_data:
                if "file_id" not in rec or not rec.get("file_id"):
                    rec["file_id"] = file_model.file_id

            return sheet_models, flat_data
        except HTTPException:
            raise
        except Exception as e:
            log_and_raise_http(500, "Ошибка при обработке листов", e)

    async def _process_sheets(
        self,
        file_id: str,
        sheets: List[dict],
        city: str,
        year: int,
        form_id: str,
        skip_sheets: Optional[List[int]] = None,
        spravochno_keywords: Optional[List[str]] = None
    ) -> Tuple[List[SheetModel], List[dict]]:
        sheet_models = []
        all_flat_data = []
        for idx, sheet in enumerate(sheets):
            name = sheet["sheet_name"].strip()
            # пропускаем по имени устоявшиеся ненужные листы
            if name in ('Раздел0', 'Лист1'):
                continue
            # проверка skip_sheets (используется индекс листа)
            if skip_sheets and isinstance(skip_sheets, list) and idx in skip_sheets:
                logger.info(f"SheetProcessor: пропускаем лист индекс={idx}, name='{name}' по skip_sheets")
                continue
            try:
                parser = get_sheet_parser(name)
                # передаём spravochno_keywords в парсер (он пробросит в NotesProcessor)
                if spravochno_keywords:
                    setattr(parser, "spravochno_keywords", spravochno_keywords)

                parsed_data = parser.parse(sheet["data"])
                flat_data = parser.generate_flat_data(year, city, name, form_id=form_id)

                if flat_data:
                    logger.info("SheetProcessor: sheet '%s' сгенерировал %d flat записей (file_id=%s)", name,
                                len(flat_data), file_id)
                    for sample in flat_data[:3]:
                        logger.debug("SheetProcessor sample flat: %s", sample)
                else:
                    logger.warning("SheetProcessor: sheet '%s' сгенерировал 0 flat записей (file_id=%s)", name, file_id)
                all_flat_data.extend(flat_data)

                sheet_model = SheetModel(
                    file_id=file_id,
                    sheet_name=name,
                    sheet_fullname=str(sheet['data'].columns[0]) if hasattr(sheet['data'], 'columns') else "",
                    year=year,
                    city=city,
                    headers=parsed_data.get("headers", {}),
                    data=parsed_data.get("data", [])
                )
                sheet_models.append(sheet_model)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Ошибка обработки раздела {name}: {str(e)}. Убедитесь в корректности структуры файла."
                )
        return sheet_models, all_flat_data


async def is_file_unique(filename: str) -> bool:
    """
    Проверяет по коллекции Files: если уже есть запись с таким filename и status == success —
    считаем дубликатом.
    """
    db = mongo_connection.get_database()
    repo = FileRepository(db.get_collection("Files"))
    existing = await repo.find_by_filename(filename)
    if not existing:
        return True
    return existing.get("status") != FileStatus.SUCCESS.value
