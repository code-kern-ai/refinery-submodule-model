from typing import Any, Dict, Optional, Tuple, List, Union
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
    CognitionProject,
    CognitionMarkdownDataset,
    CognitionMarkdownFile,
)

ETL_DIR = Path(os.getenv("ETL_DIR", "/app/data/etl"))
JSON_CHUNKS_ENDING = ".chunks.json"


def get_full_config_and_tokenizer_from_config_id(
    file_reference: FileReference,
    etl_config_id: Optional[str] = None,  # or in file_reference.meta_data
    content_type: Optional[str] = None,  # or in file_reference.content_type
    chunk_size: Optional[int] = 1000,
    # only set for chat messages
    project_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:

    if project_id and conversation_id:
        # project related load
        for_project = True

    etl_preset_item = etl_config_presets_db_co.get(
        etl_config_id or file_reference.meta_data.get("etl_config_id")
    )
    extraction_config, etl_file_type = get_extraction_config_for_file_type(
        etl_preset_item, content_type or file_reference.content_type
    )
    llm_config = {}
    if llm_indicator_extract := extraction_config.get("llmIdentifier"):
        llm_config = {
            {
                **extraction_config.get("llmConfig", {}),
                "llmIdentifier": llm_indicator_extract,
                "overwriteVisionPrompt": extraction_config.get("overwriteVisionPrompt"),
            }
        }
    full_config = [
        {
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "file_type": etl_file_type,
                "extractor": extraction_config.get("extractor"),
                "minio_path": file_reference.minio_path,
                "fallback": None,  # later filled by config of project
            },
            **llm_config,
        },
    ]

    if transformation_config := etl_preset_item.etl_config.get("transformation"):
        transformation_type = transformation_config.get("type", "NO_TRANSFORMATION")
        if transformation_type != "NO_TRANSFORMATION":
            transformation_llm_config = {
                **transformation_config.get("llmConfig", {}),
                "llmIdentifier": transformation_config.get("llmIdentifier"),
            }

            splitting_config = {
                "llm_config": transformation_llm_config,  # splitting strategy "CHUNK" needs llm_config to execute `split_large_sections_via_llm`
                "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
                "task_config": {
                    "use_cache": False,
                    "strategy": enums.ETLSplitStrategy.CHUNK.value,
                    "chunk_size": chunk_size,
                },
            }

            if transformation_type == "COMMON_ETL":
                # add default splitting for common etl

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
            elif transformation_type == "SUMMARIZE":
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
                        "use_cache": False,
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
    return full_config, etl_preset_item.etl_config.get("tokenizer")


# helper function for existing functionality, will be replaced with better builder in the future
def get_full_config_for_tmp_doc(
    file_reference: FileReference,
    project_item: CognitionProject,
    conversation_id: str,
    chunk_size: Optional[int] = 1000,
) -> List[Dict[str, Any]]:
    raise ValueError("outdated function - do not use")
    extraction_llm_config, transformation_llm_config = __get_llm_config_from_project(
        project_item
    )
    extractor = extraction_llm_config.get("extractor")
    if extractor is None:
        print(
            f"WARNING:  {__name__} - no extractor found in markdown_file meta_data for {file_reference.original_file_name}, will infer default"
        )

    full_config = [
        {
            "llm_config": extraction_llm_config,
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "extractor": extractor,
                "minio_path": file_reference.minio_path,
                "fallback": None,  # later filled by config of project
            },
        },
        {
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": chunk_size,
            },
        },
        {
            "llm_config": transformation_llm_config,
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
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "delete_queue_marker_s3": {
                    "enabled": True,
                    "path": __get_minio_path_for_deletion(
                        file_reference, str(project_item.id), conversation_id
                    ),
                },
                "copy_to_chat_files": {
                    "enabled": True,
                    "path": __get_minio_path_for_copy(
                        file_reference, str(project_item.id), conversation_id
                    ),
                },
            },
        },
    ]
    return full_config


def get_full_config_for_markdown_file(
    file_reference: FileReference,
    markdown_dataset: CognitionMarkdownDataset,
    markdown_file: CognitionMarkdownFile,
    chunk_size: Optional[int] = 1000,
) -> List[Dict[str, Any]]:
    extraction_llm_config, transformation_llm_config = __get_llm_config_from_dataset(
        markdown_dataset
    )
    extractor = markdown_file.meta_data.get("extractor")
    if extractor is None:
        print(
            f"WARNING:  {__name__} - no extractor found in markdown_file meta_data for {file_reference.original_file_name}, will infer default"
        )

    full_config = [
        {
            "llm_config": extraction_llm_config,
            "task_type": enums.CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": True,
                "extractor": extractor,
                "minio_path": file_reference.minio_path,
                "fallback": None,  # later filled by config of project
            },
        },
        {
            "llm_config": extraction_llm_config,
            "task_type": enums.CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": True,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": chunk_size,
            },
        },
        {
            "llm_config": transformation_llm_config,
            "task_type": enums.CognitionMarkdownFileState.TRANSFORMING.value,
            "task_config": {
                "use_cache": True,
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
        },
        {
            "task_type": enums.CognitionMarkdownFileState.LOADING.value,
            "task_config": {
                "markdown_file": {
                    "enabled": True,
                    "id": str(markdown_file.id),
                },
            },
        },
    ]
    return full_config


def __get_llm_config_from_project(
    project_item: CognitionProject,
) -> Tuple[Dict[str, Any], str]:
    extraction_llm_config = project_item.llm_config.get("extraction", {})
    transformation_llm_config = project_item.llm_config.get("transformation", {})
    if not extraction_llm_config or not transformation_llm_config:
        raise ValueError(f"project with id {project_item.id} has incomplete llm_config")

    return extraction_llm_config, transformation_llm_config


def __get_llm_config_from_dataset(
    markdown_dataset: CognitionMarkdownDataset,
) -> Tuple[Dict[str, Any], str]:
    extraction_llm_config = markdown_dataset.llm_config.get("extraction", {})
    transformation_llm_config = markdown_dataset.llm_config.get("transformation", {})
    if not extraction_llm_config or not transformation_llm_config:
        raise ValueError(
            f"Dataset with id {markdown_dataset.id} has incomplete llm_config"
        )

    return extraction_llm_config, transformation_llm_config


def get_full_config_for_integration(
    integration: CognitionIntegration,
    record: IntegrationSharepoint,
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
                    {
                        "enabled": True,
                        "name": enums.ETLTransformer.SUMMARIZE.value,
                        "system_prompt": None,
                        "user_prompt": " ".join(
                            (
                                "You are a helpful AI assistant that summarizes documents.",
                                "Your task is to provide a concise summary of the provided text.",
                                "You will be given a context, and you should summarize it in a clear and concise manner.",
                                "The summary should capture the main points and key information from the context.",
                                (
                                    f"You are summarizing the list of file paths in folder `{record.parent_path}`."
                                    if record.extension == "FOLDER"
                                    else f"You are summarizing the file `{record.name}` in folder `{record.parent_path}`."
                                ),
                                f"IT IS CRUCIAL THAT YOU ONLY ANSWER IN ISO-639-1:{integration.tokenizer[:2]}",
                            )
                        ),
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
        {
            "task_type": enums.CognitionMarkdownFileState.NOTIFYING.value,
            "task_config": {
                "http": [
                    {
                        "url": "http://cognition-integration-provider:80/etl/status/{integration_id}",
                        "method": "PUT",
                    }
                ]
            },
        },
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

        if llm_config.get("overwriteVisionPrompt"):
            prompt_hash = get_hashed_string(llm_config.get("overwriteVisionPrompt", ""))
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

        if llm_config.get("overwriteVisionPrompt"):
            prompt_hash = get_hashed_string(llm_config.get("overwriteVisionPrompt", ""))
            extraction_key = extraction_key / prompt_hash
        else:
            extraction_key = extraction_key / "DEFAULT_PROMPT"

    return extraction_key


def get_transformation_key(
    org_id: str,
    download_id: str,
    extractor: enums.ETLExtractorPDF,
    llm_config: Dict[str, Any],
) -> Path:
    llm_identifier = enums.LLMProvider.from_string(llm_config.get("llmIdentifier"))
    transformation_key = (
        Path(org_id) / download_id / "transform" / llm_identifier.as_key()
    )

    if llm_identifier == enums.LLMProvider.AZURE:
        engine = llm_config.get("engine", "")
        api_base = llm_config.get("apiBase", "")
        api_version = llm_config.get("apiVersion", "")
        api_hash = get_hashed_string(extractor.value, api_base, api_version)
        transformation_key = transformation_key / engine / api_hash
    elif llm_identifier == enums.LLMProvider.AZURE_FOUNDRY:
        model = llm_config.get("model", "")
        api_hash = get_hashed_string(extractor.value, llm_config.get("apiBase", ""))
        transformation_key = transformation_key / model / api_hash
    elif (
        llm_identifier == enums.LLMProvider.OPENAI
        or llm_identifier == enums.LLMProvider.PRIVATEMODE_AI
    ):
        model = llm_config.get("model")
        extractor_hash = get_hashed_string(extractor.value)
        transformation_key = transformation_key / model / extractor_hash
    return transformation_key


def get_hashed_string(*args, delimiter: str = "_") -> str:
    hash_string = delimiter.join(map(str, args))
    hasher = hashlib.new("sha256")
    hasher.update(hash_string.encode())
    return hasher.hexdigest()


def delete_etl_cache(org_id: str, download_id: str) -> None:
    def rm_tree(path: Path):
        for item in path.iterdir():
            if item.is_dir():
                rm_tree(item)
            else:
                item.unlink()
        path.rmdir()

    etl_cache_dir = ETL_DIR / org_id / download_id
    if etl_cache_dir.exists() and etl_cache_dir.is_dir():
        rm_tree(etl_cache_dir)


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
