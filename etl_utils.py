from typing import Any, Dict, List, Optional, Tuple
from .enums import (
    ETLFileType,
    ETLExtractorMD,
    ETLExtractorPDF,
    CognitionMarkdownFileState,
)
from .cognition_objects import project as project_db_co


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


def get_etl_config_from_project_id(project_id: str) -> Tuple[Dict[str, Any], str]:
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
