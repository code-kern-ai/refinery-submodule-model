from typing import Any, Dict, Optional, Tuple
from pathlib import Path

import hashlib
import os

from . import enums
from .global_objects.etl_task import DEFAULT_EXTRACTORS
from .models import (
    FileReference,
    CognitionIntegration,
    IntegrationSharepoint,
    CognitionProject,
)

ETL_DIR = os.getenv("ETL_DIR", "/app/data/etl")
JSON_CHUNKS_ENDING = ".chunks.json"


# helper function for existing functionality, will be replaced with better builder in the future
def get_full_config_for_tmp_doc(
    project_item: CognitionProject,
    file_reference: FileReference,
    meta_data: Optional[Dict[str, Any]] = None,
    file_name: Optional[str] = None,
) -> Dict[str, Any]:
    extraction_llm_config, transformation_llm_config = __get_etl_config_from_project(
        project_item
    )
    extractor = extraction_llm_config.get("extractor")

    full_config = [
        {
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "file_type": enums.ETLFileType.PDF.value,  # fixed for tmp doc atm
                "extractor": extractor,
                "minio_path": file_reference.minio_path,
                "fallback": None,  # later filled by config of project
            },
            "llm_config": extraction_llm_config,
        },
        {
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": 1000,
            },
        },
        {
            "task_type": enums.CognitionMarkdownFileState.TRANSFORMING.value,
            "task_config": {
                "use_cache": False,
                "transformers": [
                    {  # NOTE: __call_gpt_with_key only reads user_prompt
                        "enabled": False,
                        "name": enums.ETLTransformer.CLEANSE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                    {
                        "enabled": True,
                        "name": enums.ETLTransformer.TEXT_TO_TABLE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                    {
                        "enabled": False,
                        "name": enums.ETLTransformer.SUMMARIZE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                ],
            },
            "llm_config": transformation_llm_config,
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "delete_queue_marker_s3": {
                    "enabled": True,
                    "path": __get_minio_path_for_deletion(
                        file_reference, meta_data, file_name
                    ),
                },
                "copy_to_chat_files": {
                    "enabled": True,
                    "path": __get_minio_path_for_copy(
                        file_reference, meta_data, file_name
                    ),
                },
            },
        },
    ]
    return full_config


def __get_etl_config_from_project(
    project_item: CognitionProject,
) -> Tuple[Dict[str, Any], str]:
    extraction_llm_config = project_item.llm_config.get("extraction", {})
    transformation_llm_config = project_item.llm_config.get("transformation", {})
    if not extraction_llm_config or not transformation_llm_config:
        raise ValueError(f"Project with id {project_item.ud} has incomplete llm_config")

    return extraction_llm_config, transformation_llm_config


def get_full_config_for_integration(
    integration: CognitionIntegration,
    record: IntegrationSharepoint,
):
    file_type = enums.ETLFileType.from_string(
        record.extension.replace(".", "").replace("FOLDER", "md")
    )

    extractor = DEFAULT_EXTRACTORS.get(file_type, enums.ETLExtractorMD.FILESYSTEM)

    full_config = [
        {
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "file_type": file_type.value,
                "extractor": extractor.value,
                "fallback": [
                    {
                        "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
                        "task_config": {
                            "use_cache": False,
                            "file_type": file_type.value,
                            "extractor": enums.ETLExtractorPDF.VISION.value,
                        },
                        "llm_config": integration.llm_config,
                    }
                ],
            },
            "llm_config": integration.llm_config,
        },
        {
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.SHRINK.value,
                "chunk_size": integration.config.get("split_kwargs", {}).get(
                    "chunk_size", 16384
                ),
                "keep_first_n": integration.config.get("split_kwargs", {}).get(
                    "keep_first_n", 5
                ),
                "keep_last_n": integration.config.get("split_kwargs", {}).get(
                    "keep_last_n", 1
                ),
            },
            "llm_config": integration.llm_config,
        },
        {
            "task_type": enums.CognitionMarkdownFileState.TRANSFORMING.value,
            "task_config": {
                "use_cache": False,
                "transformers": [
                    {  # NOTE: __call_gpt_with_key only reads user_prompt
                        "enabled": False,
                        "name": enums.ETLTransformer.CLEANSE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                    {
                        "enabled": False,
                        "name": enums.ETLTransformer.TEXT_TO_TABLE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                    {
                        "enabled": True,
                        "name": enums.ETLTransformer.SUMMARIZE.value,
                        "system_prompt": None,
                        "user_prompt": None,
                    },
                ],
            },
            "llm_config": integration.llm_config,
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "refinery_project": {
                    "enabled": True,
                    "id": str(integration.project_id),
                },
                "markdown_file": {
                    "enabled": False,
                    "id": None,
                },
            },
        },
        {
            "task_type": enums.CognitionMarkdownFileState.NOTIFYING.value,
            "task_config": {
                "http": {
                    "url": "http://cognition-integration-provider:80/etl/finished",
                    "method": "POST",
                }
            },
        },
    ]

    return full_config


def __get_minio_path_for_deletion(
    file_reference: FileReference,
    meta_data: Optional[Dict[str, Any]] = None,
    file_name: Optional[str] = None,
) -> str:
    project_id = (meta_data or file_reference.meta_data).get("project_id")
    conversation_id = (meta_data or file_reference.meta_data).get("conversation_id")
    original_file_name = file_name or file_reference.original_file_name
    return f"_cognition/{project_id}/chat_tmp_files/{conversation_id}/queued/{original_file_name}.info"


def __get_minio_path_for_copy(
    file_reference: FileReference,
    meta_data: Optional[Dict[str, Any]] = None,
    file_name: Optional[str] = None,
) -> str:
    project_id = (meta_data or file_reference.meta_data).get("project_id")
    conversation_id = (meta_data or file_reference.meta_data).get("conversation_id")
    original_file_name = file_name or file_reference.original_file_name
    return f"_cognition/{project_id}/chat_tmp_files/{conversation_id}/{original_file_name}{JSON_CHUNKS_ENDING}"


def get_download_key(org_id: str, download_id: str) -> Path:
    return Path(org_id, "download", download_id)


def get_extraction_key(
    org_id: str, extractor: enums.ETLExtractorPDF, llm_config: Dict[str, Any]
) -> Path:
    extraction_key = Path(org_id) / "extract" / extractor.value

    if extractor == enums.ETLExtractorPDF.AZURE_DI and llm_config:
        azure_di_api_base = llm_config.get("azureDiApiBase")
        api_hash = __get_hashed_string(azure_di_api_base)
        extraction_key = extraction_key / api_hash
    elif extractor == enums.ETLExtractorPDF.VISION and llm_config:
        llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
        extraction_key = extraction_key / llm_identifier.as_key()

        if llm_identifier == enums.LLMProvider.AZURE:
            engine = llm_config.get("engine", "")
            api_base = llm_config.get("apiBase", "")
            api_version = llm_config.get("apiVersion", "")
            api_hash = __get_hashed_string(api_base, api_version)
            extraction_key = extraction_key / engine / api_hash
        elif llm_identifier == enums.LLMProvider.OPENAI:
            model = llm_config.get("model")
            extraction_key = extraction_key / model

        if llm_config.get("overwriteVisionPrompt"):
            prompt_hash = __get_hashed_string(
                llm_config.get("overwriteVisionPrompt", "")
            )
            extraction_key = extraction_key / prompt_hash
        else:
            extraction_key = extraction_key / "DEFAULT_PROMPT"
    return extraction_key


def get_transformation_key(org_id: str, llm_config: Dict[str, Any]) -> Path:
    llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
    transformation_key = Path(org_id) / "transform" / llm_identifier.as_key()

    if llm_identifier == enums.LLMProvider.AZURE:
        engine = llm_config.get("engine", "")
        api_base = llm_config.get("apiBase", "")
        api_version = llm_config.get("apiVersion", "")
        api_hash = __get_hashed_string(api_base, api_version)
        transformation_key = transformation_key / engine / api_hash
    elif (
        llm_identifier == enums.LLMProvider.OPENAI
        or llm_identifier == enums.LLMProvider.PRIVATEMODE_AI
    ):
        model = llm_config.get("model")
        transformation_key = transformation_key / model
    elif llm_identifier == enums.LLMProvider.AZURE_FOUNDRY:
        model = llm_config.get("model", "")
        api_hash = __get_hashed_string(llm_config.get("apiBase", ""))
        transformation_key = transformation_key / model / api_hash
    return transformation_key


def __get_hashed_string(*args, delimiter: str = "_") -> str:
    hash_string = delimiter.join(map(str, args))
    hasher = hashlib.new("sha256")
    hasher.update(hash_string.encode())
    return hasher.hexdigest()
