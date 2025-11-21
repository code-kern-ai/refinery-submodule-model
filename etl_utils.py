from typing import Any, Dict, List, Optional, Tuple
from .enums import (
    ETLExtractorPDF,
    CognitionMarkdownFileState,
    LLMProvider,
    ETLExtractorPDF,
)
from .global_objects.etl_task import DEFAULT_EXTRACTORS, DEFAULT_FALLBACK_EXTRACTORS
from .models import (
    FileReference,
    CognitionIntegration,
    IntegrationSharepoint,
    CognitionProject,
)
from . import enums
import hashlib

JSON_CHUNKS_ENDING = ".chunks.json"


# helper function for existing functionality, will be replaced with better builder in the future
def get_full_config_for_tmp_doc(
    project_item: CognitionProject,
    file_reference: FileReference,
    meta_data: Optional[Dict[str, Any]] = None,
    file_name: Optional[str] = None,
) -> Dict[str, Any]:
    extraction_config, transformation_config = __get_etl_config_from_project(
        project_item
    )
    extractor = extraction_config.get("extractor")

    full_config = [
        {
            "task_type": CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "file_type": enums.ETLFileType.PDF.value,  # fixed for tmp doc atm
                "extractor": extractor,
                "minio_path": file_reference.minio_path,
                "original_file_name": file_reference.original_file_name,
                "fallback": None,  # later filled by config of project
            },
            "llm_config": extraction_config,
        },
        {
            "task_type": CognitionMarkdownFileState.SPLITTING.value,
            "task_config": {
                "use_cache": False,
                "strategy": enums.ETLSplitStrategy.CHUNK.value,
                "chunk_size": 1000,
            },
        },
        {
            "task_type": CognitionMarkdownFileState.TRANSFORMING.value,
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
            "llm_config": transformation_config,
        },
        {
            "task_type": CognitionMarkdownFileState.LOADING.value,
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
    extraction_config = project_item.llm_config.get("extraction", {})
    transformation_config = project_item.llm_config.get("transformation", {})
    if not extraction_config or not transformation_config:
        raise ValueError(f"Project with id {project_item.ud} has incomplete llm_config")

    return extraction_config, transformation_config


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
            "task_type": CognitionMarkdownFileState.EXTRACTING.value,
            "task_config": {
                "use_cache": False,
                "file_type": file_type.value,
                "extractor": extractor.value,
                "fallback": [
                    {
                        "task_type": CognitionMarkdownFileState.EXTRACTING.value,
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
            "task_type": CognitionMarkdownFileState.SPLITTING.value,
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
            "task_type": CognitionMarkdownFileState.TRANSFORMING.value,
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
            "task_type": CognitionMarkdownFileState.LOADING.value,
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
            "task_type": CognitionMarkdownFileState.NOTIFYING.value,
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
