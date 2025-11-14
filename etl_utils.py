from typing import Any, Dict, List, Optional, Tuple
from .enums import (
    ETLFileType,
    ETLExtractorMD,
    ETLExtractorPDF,
    CognitionMarkdownFileState,
)
from .cognition_objects import project as project_db_co
from .models import FileReference
from . import enums


# helper function for existing functionality, will be replaced with better builder in the future
def create_etl_task_config_from_file_reference_tmp_doc(
    file_reference: FileReference,
) -> Dict[str, Any]:
    project_config, tokenizer = __get_etl_config_from_project_id(
        file_reference.meta_data.get("project_id")
    )
    task_config = __create_etl_config_for_tmp_doc(
        extract_config={
            "file_type": enums.ETLFileType.PDF.value,  # fixed for tmp doc atm
            "fallback": None,  # later filled by config of project
            "minio_path": file_reference.minio_path,
            "original_file_name": file_reference.original_file_name,
            "cache_config": {
                enums.ETLCacheKeys.FILE_CACHE.value: True,
                enums.ETLCacheKeys.EXTRACTION.value: {
                    "file_reference_id": str(file_reference.id)
                    # if exists file extraction id
                },
            },
        },
        split_config={
            "strategy": enums.ETLSplitStrategy.CHUNK.value,
            "chunk_size": 1000,
        },
        transform_config={
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
                    "file_reference_id": str(file_reference.id)
                    # if exists file extraction id
                    # if exists file transformation id
                },
            },
        },
        **project_config,
    )
    task_config.extend(
        [
            {
                "task_type": enums.CognitionMarkdownFileState.LOADING.value,
                "delete_queue_marker_s3": __get_minio_path_for_deletion(file_reference),
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
        # add notify for websocket (probably to cognition-gateway or let gateway check & send wes on complete poll)
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
) -> str:
    project_id = file_reference.meta_data.get("project_id")
    conversation_id = file_reference.meta_data.get("conversation_id")
    original_file_name = file_reference.original_file_name
    return f"_cognition/{project_id}/chat_tmp_files/{conversation_id}/queued/{original_file_name}.info"
