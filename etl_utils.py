from typing import Any, Dict, List, Optional
from .enums import (
    ETLFileType,
    ETLExtractorMD,
    ETLExtractorPDF,
    CognitionMarkdownFileState,
)


def create_etl_task_config() -> Dict[str, Any]:
    # TODO: small helper to create etl task config dict fit for the etl container
    # this method should be called from e.g. cognition etl or tmp doc
    pass
    # file_type = enums.ETLFileType.from_string(markdown_file.category_origin)
    # extractor = enums.ETLExtractorPDF.from_string(extractor)
    # fallback_extractors = list(
    #     filter(
    #         lambda x: x != extractor,
    #         (fallback_extractors or DEFAULT_FALLBACK_EXTRACTORS.get(file_type, [])),
    #     )
    # )
    # {
    #         "file_type": file_type.value,
    #         "extractor": extractor.value,
    #         "fallback_extractors": [fe.value for fe in fallback_extractors],
    #         "minio_path": file_reference.minio_path,
    #         "original_file_name": file_reference.original_file_name,
    #     }


# def get_or_create_markdown_file_etl_task(
#     org_id: str,
#     file_reference: FileReference,
#     markdown_file: CognitionMarkdownFile,
#     markdown_dataset: CognitionMarkdownDataset,
#     extractor: str,
#     cache_config: Dict,
#     split_config: Dict,
#     transform_config: Dict,
#     load_config: Dict,
#     notify_config: Dict,
#     priority: Optional[int] = -1,
#     fallback_extractors: Optional[list[enums.ETLExtractorPDF]] = [],
# ) -> EtlTask:
#     if etl_task := (
#         session.query(EtlTask).filter(EtlTask.id == markdown_file.etl_task_id).first()
#     ):
#         return etl_task

#     file_type = enums.ETLFileType.from_string(markdown_file.category_origin)
#     extractor = enums.ETLExtractorPDF.from_string(extractor)
#     fallback_extractors = list(
#         filter(
#             lambda x: x != extractor,
#             (fallback_extractors or DEFAULT_FALLBACK_EXTRACTORS.get(file_type, [])),
#         )
#     )

#     return create(
#         org_id=org_id,
#         user_id=markdown_file.created_by,
#         file_size_bytes=file_reference.file_size_bytes,
#         cache_config=cache_config,
#         extract_config={
#             "file_type": file_type.value,
#             "extractor": extractor.value,
#             "fallback_extractors": [fe.value for fe in fallback_extractors],
#             "minio_path": file_reference.minio_path,
#             "original_file_name": file_reference.original_file_name,
#         },
#         split_config=split_config,
#         transform_config=transform_config,
#         load_config=load_config,
#         notify_config=notify_config,
#         llm_config=markdown_dataset.llm_config,
#         tokenizer=markdown_dataset.tokenizer,
#         priority=priority,
#     )


## helper function for existing functionality, will be replaced with better builder in the future
def create_etl_config_for_tmp_doc(**kwargs) -> List[Dict[str, Any]]:
    # # transformers is an array of dicts with name and prompt and enabled?
    # {"transformers": [{"name": "dddd"}], "llm_config": {}}
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

    return [
        config["extract"],
        config["transform"],
        config["split"],
        config["load"],
        config["notify"],
    ]
