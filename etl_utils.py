from typing import Any, Dict, Optional, Tuple, List
from pathlib import Path

import hashlib
import os

from . import enums
from .cognition_objects import etl_config_presets as etl_config_presets_db_co
from .models import (
    ETLConfigPresets,
    FileReference,
    CognitionIntegration,
    IntegrationSharepoint,
    IntegrationWebpage,
)

ETL_DIR = Path(os.getenv("ETL_DIR", "/app/data/etl"))
JSON_CHUNKS_ENDING = ".chunks.json"


def get_full_config_and_tokenizer_from_config_id(
    file_reference: FileReference,
    etl_config_id: Optional[str] = None,  # or in file_reference.meta_data
    content_type: Optional[str] = None,  # or in file_reference.content_type
    chunk_size: Optional[int] = 1000,
    # only set for markdown datasets
    markdown_file_id: Optional[str] = None,  # or in file_reference.meta_data
    # only set for chat messages
    project_id: Optional[str] = None,  # or in file_reference.meta_data
    conversation_id: Optional[str] = None,  # or in file_reference.meta_data
    rows_per_section: Optional[
        int
    ] = 50,  # only applies to JSON/EXCEL/CSV/TSV files, default to 50 rows per section
) -> Tuple[Dict[str, Any], str]:
    for_dataset = False
    for_project = False
    if project_id and conversation_id:
        # project related load
        for_project = True
    elif markdown_file_id:
        # dataset related load
        for_dataset = True

    etl_preset_item = etl_config_presets_db_co.get(
        etl_config_id or file_reference.meta_data.get("etl_config_id")
    )
    extraction_config, etl_file_type = get_extraction_config_for_file_type(
        etl_preset_item, content_type or file_reference.content_type
    )
    llm_config = {}
    if llm_indicator_extract := extraction_config.get("llmIdentifier"):
        llm_config = {
            **extraction_config.get("llmConfig", {}),
            "llmIdentifier": llm_indicator_extract,
            "overwriteVisionPrompt": extraction_config.get("overwriteVisionPrompt"),
        }
    elif extraction_config.get("azureDiApiBase"):
        llm_config = {
            "azureDiApiBase": extraction_config["azureDiApiBase"],
            "azureDiEnvVarId": extraction_config["azureDiEnvVarId"],
        }
    full_config = [
        {
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": True,
                "file_type": etl_file_type,
                "extractor": extraction_config.get("extractor"),
                "minio_path": file_reference.minio_path,
                "fallback": None,  # later filled by config of project
            },
            "llm_config": llm_config,
        },
    ]

    if transformation_config := etl_preset_item.etl_config.get("transformation"):
        transformation_type = enums.ETLTransformerType.from_string(
            transformation_config.get("type", "NO_TRANSFORMATION")
        )
        if transformation_type != enums.ETLTransformerType.NO_TRANSFORMATION:
            transformation_llm_config = {
                **transformation_config.get("llmConfig", {}),
                "llmIdentifier": transformation_config.get("llmIdentifier"),
            }

            # splitting strategy "CHUNK" needs llm_config to execute `split_large_sections_via_llm`
            splitting_config = {
                "llm_config": transformation_llm_config,
                "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
                "task_config": {
                    "use_cache": True,
                    "strategy": enums.ETLSplitStrategy.CHUNK.value,
                    "chunk_size": chunk_size,
                    "rows_per_section": rows_per_section,
                },
            }

            if transformation_type == enums.ETLTransformerType.COMMON_ETL:
                full_config.append(splitting_config)
                transformers = [
                    {  # NOTE: __call_gpt_with_key only reads user_prompt
                        "enabled": True,
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
                ]
            elif transformation_type == enums.ETLTransformerType.SUMMARIZE:
                full_config.append(splitting_config)
                transformers = [
                    {
                        "enabled": True,
                        "name": enums.ETLTransformer.SUMMARIZE.value,
                        "system_prompt": None,
                        "user_prompt": transformation_config.get("summarizationPrompt"),
                    },
                ]
            else:
                transformers = []

            full_config.append(
                {
                    "llm_config": transformation_llm_config,
                    "task_type": enums.CognitionMarkdownFileState.TRANSFORMING.value,
                    "task_config": {
                        "use_cache": True,
                        "transformation_type": transformation_type.value,
                        "transformers": transformers,
                    },
                }
            )

    if for_project:
        full_config.append(
            {
                "task_type": enums.CognitionMarkdownFileState.LOADING.value,
                "task_config": {
                    "delete_queue_marker_s3": {
                        "enabled": True,
                        "path": __get_minio_path_for_deletion(
                            file_reference, project_id, conversation_id
                        ),
                    },
                    "copy_to_chat_files": {
                        "enabled": True,
                        "path": __get_minio_path_for_copy(
                            file_reference, project_id, conversation_id
                        ),
                    },
                },
            },
        )
    elif for_dataset:
        full_config.append(
            {
                "task_type": enums.CognitionMarkdownFileState.LOADING.value,
                "task_config": {
                    "markdown_file": {
                        "enabled": True,
                        "id": (
                            markdown_file_id
                            or file_reference.meta_data["markdown_file_id"]
                        ),
                    }
                },
            },
        )
    return full_config, etl_preset_item.etl_config.get("tokenizer")


def get_full_config_for_webpage_integration(
    integration: CognitionIntegration,
    record: IntegrationWebpage,
    rows_per_section: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    full_config = [
        {
            "llm_config": integration.llm_config,
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "fallback": None,
            },
        },
        {
            "llm_config": integration.llm_config,
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": 1000,  # TODO: chunk size doesn't work well with rows_per_section so it isn't evaluated for json,csv,excel structured files
                "rows_per_section": rows_per_section,
            },
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "integration_record": {
                    "enabled": True,
                    "id": str(record.id),
                    "integration_id": str(integration.id),
                },
                "markdown_file": {
                    "enabled": False,
                    "id": None,
                },
            },
        },
        # {
        #     "task_type": enums.CognitionMarkdownFileState.NOTIFYING.value,
        #     "task_config": {
        #         "integration": [
        #             {
        #                 "integration_id": str(integration.id),
        #             }
        #         ]
        #     },
        # },
    ]
    return full_config


def get_full_config_for_sharepoint_integration(
    integration: CognitionIntegration,
    record: IntegrationSharepoint,
    rows_per_section: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    full_config = [
        {
            "llm_config": integration.llm_config,
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "fallback": None,
            },
        },
        {
            "llm_config": integration.llm_config,
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": 1000,
                "rows_per_section": rows_per_section,
                # "keep_first_n": integration.config.get("split_kwargs", {}).get(
                #     "keep_first_n", 5
                # ),
                # "keep_last_n": integration.config.get("split_kwargs", {}).get(
                #     "keep_last_n", 1
                # ),
            },
        },
        {
            "llm_config": integration.llm_config,
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
                ],
            },
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "integration_record": {
                    "enabled": True,
                    "id": str(record.id),
                    "integration_id": str(integration.id),
                },
                "markdown_file": {
                    "enabled": False,
                    "id": None,
                },
            },
        },
        # {
        #     "task_type": enums.CognitionMarkdownFileState.NOTIFYING.value,
        #     "task_config": {
        #         "integration": [
        #             {
        #                 "integration_id": str(integration.id),
        #             }
        #         ]
        #     },
        # },
    ]

    return full_config


def __get_minio_path_for_deletion(
    file_reference: FileReference,
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> str:
    project_id = project_id or file_reference.meta_data.get("project_id")
    conversation_id = conversation_id or file_reference.meta_data.get("conversation_id")
    if not project_id or not conversation_id:
        raise ValueError(
            "ERROR:    __get_minio_path_for_deletion - missing project_id or conversation_id"
        )
    return f"_cognition/{project_id}/chat_tmp_files/{conversation_id}/queued/{file_reference.original_file_name}.info"


def __get_minio_path_for_copy(
    file_reference: FileReference,
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> str:
    project_id = project_id or file_reference.meta_data.get("project_id")
    conversation_id = conversation_id or file_reference.meta_data.get("conversation_id")
    if not project_id or not conversation_id:
        raise ValueError(
            "ERROR:    __get_minio_path_for_copy - missing project_id or conversation_id"
        )
    return f"_cognition/{project_id}/chat_tmp_files/{conversation_id}/{file_reference.original_file_name}{JSON_CHUNKS_ENDING}"


def delete_etl_cache(org_id: str, download_id: str) -> None:
    def rm_tree(path: Path):
        for item in path.iterdir():
            if item.is_dir():
                rm_tree(item)
            else:
                item.unlink(missing_ok=True)
        path.rmdir()

    etl_cache_dir = ETL_DIR / org_id / download_id
    if etl_cache_dir.exists() and etl_cache_dir.is_dir():
        rm_tree(etl_cache_dir)


def get_download_key(org_id: str, download_id: str) -> Path:
    return Path(org_id) / download_id / "download"


def get_extraction_key(
    org_id: str,
    download_id: str,
    extractor: enums.ETLExtractorPDF,
    llm_config: Dict[str, Any],
) -> Path:
    extraction_key = Path(org_id) / download_id / "extract" / extractor.value

    if extractor == enums.ETLExtractorPDF.AZURE_DI and llm_config:
        azure_di_api_base = llm_config.get("azureDiApiBase")
        api_hash = get_hashed_string(azure_di_api_base)
        extraction_key = extraction_key / api_hash
    elif extractor == enums.ETLExtractorPDF.VISION and llm_config:
        llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
        extraction_key = extraction_key / llm_identifier.as_key()

        if llm_identifier == enums.LLMProvider.AZURE:
            engine = llm_config.get("engine", "")
            api_base = llm_config.get("apiBase", "")
            api_version = llm_config.get("apiVersion", "")
            api_hash = get_hashed_string(api_base, api_version)
            extraction_key = extraction_key / engine / api_hash
        elif llm_identifier == enums.LLMProvider.OPENAI:
            model = llm_config.get("model")
            extraction_key = extraction_key / model

        if overwrite_vision_prompt := llm_config.get("overwriteVisionPrompt"):
            prompt_hash = get_hashed_string(overwrite_vision_prompt)
            extraction_key = extraction_key / prompt_hash
        else:
            extraction_key = extraction_key / "DEFAULT_PROMPT"

    return extraction_key


def get_splitting_key(
    org_id: str,
    download_id: str,
    extractor: enums.ETLExtractorPDF,
    llm_config: Optional[Dict[str, Any]] = None,
) -> Path:
    extraction_key = Path(org_id) / download_id / "split" / extractor.value

    if llm_config:
        llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
        extraction_key = extraction_key / llm_identifier.as_key()

        if llm_identifier == enums.LLMProvider.AZURE:
            engine = llm_config.get("engine", "")
            api_base = llm_config.get("apiBase", "")
            api_version = llm_config.get("apiVersion", "")
            api_hash = get_hashed_string(api_base, api_version)
            extraction_key = extraction_key / engine / api_hash
        elif llm_identifier == enums.LLMProvider.OPENAI:
            model = llm_config.get("model")
            extraction_key = extraction_key / model

        if overwrite_vision_prompt := llm_config.get("overwriteVisionPrompt"):
            prompt_hash = get_hashed_string(overwrite_vision_prompt)
            extraction_key = extraction_key / prompt_hash
        else:
            extraction_key = extraction_key / "DEFAULT_PROMPT"

    return extraction_key


def get_transformation_key(
    org_id: str,
    download_id: str,
    extractor: enums.ETLExtractorPDF,
    llm_config: Dict[str, Any],
    prompt: Optional[str] = "",
    transformation_type: Optional[
        enums.ETLTransformerType
    ] = enums.ETLTransformerType.NO_TRANSFORMATION,
) -> Path:
    llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
    transformation_key = (
        Path(org_id)
        / download_id
        / "transform"
        / transformation_type.value
        / llm_identifier.as_key()
    )

    if llm_identifier == enums.LLMProvider.AZURE:
        engine = llm_config.get("engine", "")
        api_base = llm_config.get("apiBase", "")
        api_version = llm_config.get("apiVersion", "")
        api_hash = get_hashed_string(extractor.value, api_base, api_version, prompt)
        transformation_key = transformation_key / engine / api_hash
    elif llm_identifier == enums.LLMProvider.AZURE_FOUNDRY:
        model = llm_config.get("model", "")
        api_hash = get_hashed_string(
            extractor.value, llm_config.get("apiBase", ""), prompt
        )
        transformation_key = transformation_key / model / api_hash
    elif (
        llm_identifier == enums.LLMProvider.OPENAI
        or llm_identifier == enums.LLMProvider.PRIVATEMODE_AI
    ):
        model = llm_config.get("model")
        extractor_hash = get_hashed_string(extractor.value, prompt)
        transformation_key = transformation_key / model / extractor_hash

    return transformation_key


def get_hashed_string(*args, delimiter: str = "_", from_bytes: bool = False) -> str:
    if not from_bytes:
        _hash = delimiter.join(map(str, args)).encode()
    else:
        try:
            _hash = next(map(bytes, args))
        except StopIteration:
            raise ValueError("ERROR: A 'bytes' argument is required to hash")

    hasher = hashlib.sha256(_hash)
    return hasher.hexdigest()


def get_extraction_config_for_file_type(
    preset: ETLConfigPresets, content_type: str
) -> Tuple[Dict[str, Any], str]:
    file_type = enums.ETLFileType.from_mimetype(content_type).value
    access_key = file_type.lower()
    if not preset:
        raise ValueError("ETL Config Preset not found")
    if file_type_config := preset.etl_config.get("extraction", {}).get(access_key):
        return file_type_config, file_type
    return preset.etl_config.get("extraction", {}).get("default", {}), file_type
