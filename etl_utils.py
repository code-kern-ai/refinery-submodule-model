from typing import Any, Dict, List, Optional, Tuple
from .enums import (
    ETLExtractorPDF,
    CognitionMarkdownFileState,
    LLMProvider,
    ETLExtractorPDF,
)
from .cognition_objects import project as project_db_co
from .models import FileReference
from . import enums
import hashlib

JSON_CHUNKS_ENDING = ".chunks.json"


# helper function for existing functionality, will be replaced with better builder in the future
def create_etl_task_config_from_file_reference_tmp_doc(
    file_reference: FileReference,
    file_extraction_id: Optional[str] = None,
    file_transformation_id: Optional[str] = None,
    parse_scope: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    file_name: Optional[str] = None,
) -> Dict[str, Any]:
    project_config, tokenizer = __get_etl_config_from_project_id(
        (meta_data or file_reference.meta_data).get("project_id")
    )

    kwargs = {}
    if parse_scope is None or "EXTRACT" in parse_scope:
        kwargs["extract_config"] = {
            "file_type": enums.ETLFileType.PDF.value,  # fixed for tmp doc atm
            "fallback": None,  # later filled by config of project
            "minio_path": file_reference.minio_path,
            "original_file_name": file_reference.original_file_name,
            "cache_config": {
                enums.ETLCacheKeys.FILE_CACHE.value: True,
                enums.ETLCacheKeys.EXTRACTION.value: {
                    "file_reference_id": str(file_reference.id),
                },
            },
        }
        if file_extraction_id:
            kwargs["extract_config"]["cache_config"][
                enums.ETLCacheKeys.EXTRACTION.value
            ]["file_extraction_id"] = file_extraction_id

    if parse_scope is None or "TRANSFORM" in parse_scope:
        kwargs["transform_config"] = {
            "transformers": [
                {
                    "enabled": True,  # this transformer is disabled because it often hangs the ETL process
                    "name": enums.ETLTransformer.CLEANSE.value,
                },
                {
                    "enabled": True,
                    "name": enums.ETLTransformer.TEXT_TO_TABLE.value,
                },
            ],
            "cache_config": {
                enums.ETLCacheKeys.FILE_CACHE.value: True,
                enums.ETLCacheKeys.TRANSFORMATION.value: {
                    "file_reference_id": str(file_reference.id),
                },
            },
        }
        if file_extraction_id:
            kwargs["transform_config"]["cache_config"][
                enums.ETLCacheKeys.TRANSFORMATION.value
            ]["file_extraction_id"] = file_extraction_id

        if file_transformation_id:
            kwargs["transform_config"]["cache_config"][
                enums.ETLCacheKeys.TRANSFORMATION.value
            ]["file_transformation_id"] = file_transformation_id

    task_config = __create_etl_config_for_tmp_doc(
        **kwargs,
        split_config={
            "strategy": enums.ETLSplitStrategy.CHUNK.value,
            "chunk_size": 1000,
        },
        **project_config,
    )
    task_config.extend(
        [
            {
                "task_type": enums.CognitionMarkdownFileState.LOADING.value,
                "delete_queue_marker_s3": __get_minio_path_for_deletion(
                    file_reference, meta_data, file_name
                ),
                "copy_to_chat_files": __get_minio_path_for_copy(
                    file_reference, meta_data, file_name
                ),
            },
            {
                "task_type": enums.CognitionMarkdownFileState.CACHE_HANDLING.value,
                enums.ETLCacheKeys.FILE_CACHE.value: {"delete": True},
            },
        ]
    )
    return task_config, tokenizer


## helper function for existing functionality, will be replaced with better builder in the future
def __create_etl_config_for_tmp_doc(**kwargs) -> List[Dict[str, Any]]:
    config = {
        "extract": {
            "task_type": CognitionMarkdownFileState.EXTRACTING.value,
        },
        "transform": {
            "task_type": CognitionMarkdownFileState.TRANSFORMING.value,
        },
        "split": {
            "task_type": CognitionMarkdownFileState.SPLITTING.value,
        },
        "load": {
            "task_type": CognitionMarkdownFileState.LOADING.value,
        },
        "notify": {
            "task_type": CognitionMarkdownFileState.NOTIFYING.value,
        },
    }
    for k, v in kwargs.items():
        config_part, key = k.split("_", 1)
        if config_part not in config:
            raise ValueError(f"Invalid config part: {config_part}")
        if key == "config":
            config[config_part].update(v)
        else:
            config[config_part][key] = v

    final = []
    if len(config["extract"]) > 1:
        final.append(config["extract"])
    if len(config["transform"]) > 1:
        final.append(config["transform"])
    if len(config["split"]) > 1:
        final.append(config["split"])
    if len(config["load"]) > 1:
        final.append(config["load"])
    if len(config["notify"]) > 1:
        final.append(config["notify"])
    return final


def __get_etl_config_from_project_id(project_id: str) -> Tuple[Dict[str, Any], str]:
    item = project_db_co.get(project_id)
    if not item:
        raise ValueError(f"Project with id {project_id} not found")

    extraction_config = item.llm_config.get("extraction", {})
    transformation_config = item.llm_config.get("transformation", {})
    if not extraction_config or not transformation_config:
        raise ValueError(f"Project with id {project_id} has incomplete llm_config")

    ## note that parts are extended to match helper method
    to_return_dict = {}
    to_return_dict["extract_extractor"] = ETLExtractorPDF.from_string(
        extraction_config.get("extractor")
    ).value
    if to_return_dict["extract_extractor"] != ETLExtractorPDF.PDF2MD.value:
        # without extractor but what gives
        to_return_dict["extract_llm_config"] = extraction_config
    # doesn't have a dedicated type yet so we can just pass all values
    to_return_dict["transform_llm_config"] = transformation_config
    return to_return_dict, item.tokenizer


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


# def create_etl_task_for_integration_provider(file_path:str, provider:str) -> List[Dict[str, Any]]:
#     task_config = __create_etl_config_for_tmp_doc(
#         extract_config={
#             "file_type": enums.ETLFileType.INTEGRATION_PROVIDER.value,
#             "provider": provider,
#             "minio_path": file_path,
#             "cache_config": {
#                 enums.ETLCacheKeys.FILE_CACHE.value: False,
#                 enums.ETLCacheKeys.EXTRACTION.value: {},
#             },
#         },
#         transform_config={
#             "transformers": [],
#             "cache_config": {
#                 enums.ETLCacheKeys.FILE_CACHE.value: False,
#                 enums.ETLCacheKeys.TRANSFORMATION.value: {},
#             },
#         },
#         split_config={
#             "strategy": enums.ETLSplitStrategy.CHUNK.value,
#             "chunk_size": 1000,
#         },
#     )
#     return task_config


def get_extraction_key(ext_method: str, extraction_llm_config: Dict[str, Any]) -> str:

    extraction_key = ext_method
    ext_method = ETLExtractorPDF.from_string(ext_method)
    if ext_method == ETLExtractorPDF.VISION and extraction_llm_config:
        llm_identifier = LLMProvider.from_string(
            extraction_llm_config.get("llmIdentifier")
        )
        extraction_key += f"_{llm_identifier.as_key()}"

        if llm_identifier == LLMProvider.AZURE:
            engine = extraction_llm_config.get("engine")
            apiVersion = extraction_llm_config.get("apiVersion")
            api_base = extraction_llm_config.get("apiBase")
            hasher = hashlib.new("sha256")
            hasher.update(f"{api_base}_{apiVersion}".encode())
            api_hash = hasher.hexdigest()
            extraction_key += f"_{engine}_{api_hash}"
        elif llm_identifier == LLMProvider.OPENAI:
            model = extraction_llm_config.get("model")
            extraction_key += f"_{model}"

        if extraction_llm_config.get("overwriteVisionPrompt"):
            hasher = hashlib.new("sha256")
            hasher.update(
                f"{extraction_llm_config.get('overwriteVisionPrompt')}".encode()
            )
            prompt_hash = hasher.hexdigest()
            extraction_key += f"_{prompt_hash}"
        else:
            extraction_key += "_DEFAULT_PROMPT"
    elif ext_method == ETLExtractorPDF.AZURE_DI and extraction_llm_config:
        azure_di_api_base = extraction_llm_config.get("azureDiApiBase")
        hasher = hashlib.new("sha256")
        hasher.update(f"{azure_di_api_base}".encode())
        api_hash = hasher.hexdigest()
        extraction_key += f"_{api_hash}"

    return extraction_key


def get_transformation_key(transformation_llm_config: Dict[str, Any]) -> str:

    llm_identifier = LLMProvider.from_string(
        transformation_llm_config.get("llmIdentifier")
    )
    transformation_key = f"{llm_identifier.as_key()}"

    if llm_identifier == LLMProvider.AZURE:
        engine = transformation_llm_config.get("engine")
        apiVersion = transformation_llm_config.get("apiVersion")
        api_base = transformation_llm_config.get("apiBase")
        hasher = hashlib.new("sha256")
        hasher.update(f"{api_base}_{apiVersion}".encode())
        api_hash = hasher.hexdigest()
        transformation_key += f"_{engine}_{api_hash}"
    elif (
        llm_identifier == LLMProvider.OPENAI
        or llm_identifier == LLMProvider.PRIVATEMODE_AI
    ):
        model = transformation_llm_config.get("model")
        transformation_key += f"_{model}"
    elif llm_identifier == LLMProvider.AZURE_FOUNDRY:
        model = transformation_llm_config.get("model")
        azure_endpoint = transformation_llm_config.get("apiBase")
        hasher = hashlib.new("sha256")
        hasher.update(f"{azure_endpoint}".encode())
        api_hash = hasher.hexdigest()
        transformation_key += f"_{api_hash}"
        transformation_key += f"_{model}"
    return transformation_key.replace("/", "_")
