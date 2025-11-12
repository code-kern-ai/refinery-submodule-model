from typing import Any, Dict


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
