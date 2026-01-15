from typing import Any, List, Optional, Dict
from enum import Enum


class EnumKern(Enum):
    @classmethod
    def all(cls):
        return [e.value for e in cls]

    @classmethod
    def from_string(cls, value: str):
        changed_value = value.upper().replace(" ", "_").replace("-", "_")
        for member in cls:
            if member.value == changed_value:
                return member
        raise ValueError(f"Unknown enum {cls.__name__}: {value}")


class DataTypes(Enum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    CATEGORY = "CATEGORY"
    TEXT = "TEXT"
    LLM_RESPONSE = "LLM_RESPONSE"
    EMBEDDING_LIST = "EMBEDDING_LIST"  # only for embeddings & default hidden
    PERMISSION = "PERMISSION"  # used for access control
    TEXT_LIST = "TEXT_LIST"
    UNKNOWN = "UNKNOWN"


class ProjectStatus(Enum):
    INIT_UPLOAD = "INIT_UPLOAD"
    INIT_COMPLETE = "INIT_COMPLETE"
    IN_DELETION = "IN_DELETION"
    INIT_SAMPLE_PROJECT = "INIT_SAMPLE_PROJECT"
    HIDDEN = "HIDDEN"


class RecordCategory(Enum):
    SCALE = "SCALE"
    TEST = "TEST"


class LabelSource(Enum):
    MANUAL = "MANUAL"
    # WEAK_SUPERVISION = Output of the Weak Supervision Model - ehemeals "programmatic"
    WEAK_SUPERVISION = "WEAK_SUPERVISION"
    INFORMATION_SOURCE = "INFORMATION_SOURCE"


class InformationSourceType(Enum):
    LABELING_FUNCTION = "LABELING_FUNCTION"
    ACTIVE_LEARNING = "ACTIVE_LEARNING"
    PRE_COMPUTED = "PRE_COMPUTED"


class InformationSourceReturnType(Enum):
    RETURN = "RETURN"
    YIELD = "YIELD"


class Notification(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class PayloadState(Enum):
    CREATED = "CREATED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class UserRoles(Enum):
    ENGINEER = "ENGINEER"
    EXPERT = "EXPERT"
    ANNOTATOR = "ANNOTATOR"


class MessageRoles(Enum):
    SYSTEM = "SYSTEM"
    USER = "USER"
    ASSISTANT = "ASSISTANT"


class LinkTypes(Enum):
    DATA_SLICE = "DATA_SLICE"
    HEURISTIC = "HEURISTIC"
    SESSION = "SESSION"


class Tablenames(Enum):
    APP_VERSION = "app_version"
    AGREEMENT = "agreement"
    USER = "user"
    ORGANIZATION = "organization"
    TEAM = "team"
    TEAM_MEMBER = "team_member"
    TEAM_RESOURCE = "team_resource"
    PROJECT = "project"
    NOTIFICATION = "notification"
    ATTRIBUTE = "attribute"
    LABELING_TASK = "labeling_task"
    LABELING_TASK_LABEL = "labeling_task_label"
    EMBEDDING = "embedding"
    EMBEDDING_TENSOR = "embedding_tensor"
    RECORD = "record"
    RECORD_TOKENIZED = "record_tokenized"
    RECORD_TOKENIZATION_TASK = "record_tokenization_task"
    RECORD_LABEL_ASSOCIATION = "record_label_association"
    RECORD_LABEL_ASSOCIATION_TOKEN = "record_label_association_token"
    RECORD_ATTRIBUTE_TOKEN_STATISTICS = "record_attribute_token_statistics"
    WEAK_SUPERVISION_TASK = "weak_supervision_task"
    WEAK_SUPERVISION_HELPER = "weak_supervision_helper"
    INFORMATION_SOURCE = "information_source"
    INFORMATION_SOURCE_STATISTICS = "information_source_statistics"
    INFORMATION_SOURCE_PAYLOAD = "information_source_payload"
    KNOWLEDGE_BASE = "knowledge_base"
    KNOWLEDGE_TERM = "knowledge_term"
    USER_SESSIONS = "user_sessions"
    UPLOAD_TASK = "upload_task"
    DATA_SLICE = "data_slice"
    DATA_SLICE_RECORD_ASSOCIATION = "data_slice_record_association"
    INFORMATION_SOURCE_STATISTICS_EXCLUSION = "information_source_statistics_exclusion"
    COMMENT_DATA = "comment_data"
    LABELING_ACCESS_LINK = "labeling_access_link"
    PERSONAL_ACCESS_TOKEN = "personal_access_token"
    PERSONAL_ACCESS_TOKEN_ETL = "personal_access_token_etl"
    PERSONAL_ACCESS_TOKEN_SCOPE_ETL = "personal_access_token_scope_etl"
    PERSONAL_ACCESS_TOKEN_ACTIVITY_LOG_ETL = "personal_access_token_activity_log_etl"
    ADMIN_MESSAGE = "admin_message"
    TASK_QUEUE = "task_queue"
    CONVERSATION = "conversation"
    MESSAGE = "message"
    STRATEGY = "strategy"
    STRATEGY_STEP = "strategy_step"
    RETRIEVER = "retriever"
    RETRIEVER_PART = "retriever_part"
    ENVIRONMENT_VARIABLE = "environment_variable"
    PIPELINE_LOGS = "pipeline_logs"
    MARKDOWN_FILE = "markdown_file"
    PYTHON_STEP = "python_step"
    LLM_STEP = "llm_step"
    MARKDOWN_DATASET = "markdown_dataset"
    WEBSOCKET_ACCESS = "websocket_access"
    CONSUMPTION_LOG = "consumption_log"
    CONSUMPTION_SUMMARY = "consumption_summary"
    MACRO = "macro"  # general definition
    MACRO_NODE = "macro_node"  # step/action of a macro
    MACRO_EDGE = "macro_edge"  # connection between steps of a macro
    MACRO_EXECUTION = "macro_execution"  # links macro id to an execution id
    MACRO_EXECUTION_LINK = "macro_execution_link"  # execution to a conversation id
    STRATEGY_REQUIREMENT = "strategy_requirement"
    STRATEGY_REQUIREMENT_MAPPING_OPTION = "strategy_requirement_mapping_option"
    MACRO_EXECUTION_SUMMARY = (
        "macro_execution_summary"  # summary of macro folder executions
    )
    CUSTOMER_BUTTON = "customer_button"
    FILE_REFERENCE = "file_reference"
    FILE_EXTRACTION = "file_extraction"
    FILE_TRANSFORMATION = "file_transformation"
    FILE_TRANSFORMATION_LLM_LOGS = "file_transformation_llm_logs"
    PIPELINE_VERSION = (
        "pipeline_version"  # dump of previous versions to easily jump between
    )
    GRAPHRAG_INDEX = "graphrag_index"
    EVALUATION_SET = "evaluation_set"
    EVALUATION_GROUP = "evaluation_group"
    EVALUATION_RUN = "evaluation_run"
    PLAYGROUND_QUESTION = "playground_question"
    FULL_ADMIN_ACCESS = "full_admin_access"
    GROUP = "group"  # used for group based access control
    GROUP_MEMBER = "group_member"  # used for group based access control
    PERMISSION = "permission"  # used for access control
    INTEGRATION = "integration"
    INTEGRATION_ACCESS = "integration_access"

    # Individial integrations
    INTEGRATION_GITHUB_FILE = "github_file"
    INTEGRATION_GITHUB_ISSUE = "github_issue"
    INTEGRATION_PDF = "pdf"
    INTEGRATION_SHAREPOINT = "sharepoint"
    STEP_TEMPLATES = "step_templates"  # templates for strategy steps
    INTEGRATION_SHAREPOINT_PROPERTY_SYNC = "sharepoint_property_sync"
    CONVERSATION_TAG = "conversation_tag"  # config of tags used in conversations
    CONVERSATION_TAG_ASSOCIATION = (
        "conversation_tag_association"  # association between conversation and tags
    )
    SUMS_TABLE = "sums_table"
    ADMIN_QUERY_MESSAGE_SUMMARY = "admin_query_message_summary"
    RELEASE_NOTIFICATION = "release_notification"
    TIMED_EXECUTIONS = "timed_executions"
    ETL_TASK = "etl_task"
    ETL_CONFIG_PRESET = "etl_config_preset"
    CONVERSATION_SHARE = "conversation_share"
    CONVERSATION_GLOBAL_SHARE = "conversation_global_share"
    INBOX_MAIL = "inbox_mail"
    INBOX_MAIL_THREAD = "inbox_mail_thread"
    INBOX_MAIL_THREAD_ASSOCIATION = "inbox_mail_thread_association"
    DATA_BLOCK = "data_block"
    DATA_BLOCK_ATTRIBUTES = "data_block_attributes"
    DATA_BLOCK_RESULTS = "data_block_results"

    def snake_case_to_pascal_case(self):
        # the type name (written in PascalCase) of a table is needed to create backrefs
        return "".join([word.title() for word in self.value.split("_")])

    def snake_case_to_camel_case(self):
        return "".join(
            [
                word.title() if idx > 0 else word.lower()
                for idx, word in enumerate(self.value.split("_"))
            ]
        )


class TeamResourceType(Enum):
    COGNITION_PROJECT = "COGNITION_PROJECT"


class CommentCategory(Enum):
    UNKNOWN = "unknown"
    LABELING_TASK = "LABELING_TASK"
    RECORD = "RECORD"
    ORGANIZATION = "ORGANIZATION"
    ATTRIBUTE = "ATTRIBUTE"
    USER = "USER"
    EMBEDDING = "EMBEDDING"
    HEURISTIC = "HEURISTIC"
    DATA_SLICE = "DATA_SLICE"
    KNOWLEDGE_BASE = "KNOWLEDGE_BASE"
    LABEL = "LABEL"

    def get_name_col(self):
        if self == CommentCategory.USER:
            return ""
        return "name"

    def get_table_name(self):
        if self == CommentCategory.USER:
            return "public.user"
        if self == CommentCategory.HEURISTIC:
            return "information_source"
        if self == CommentCategory.LABEL:
            return "labeling_task_label"
        return self.value.lower()


class CascadeBehaviour(Enum):
    KEEP_PARENT_ON_CHILD_DELETION = "KEEP_PARENT_ON_CHILD_DELETION"
    DELETE_BOTH_IF_EITHER_IS_DELETED = "DELETE_BOTH_IF_EITHER_IS_DELETED"


class LabelingTaskType(Enum):
    CLASSIFICATION = "MULTICLASS_CLASSIFICATION"
    INFORMATION_EXTRACTION = "INFORMATION_EXTRACTION"


class LabelingTaskTarget(Enum):
    ON_ATTRIBUTE = "ON_ATTRIBUTE"
    ON_WHOLE_RECORD = "ON_WHOLE_RECORD"


class EmbeddingType(Enum):
    ON_ATTRIBUTE = "ON_ATTRIBUTE"
    ON_TOKEN = "ON_TOKEN"


class UploadStates(Enum):
    """
    Explanation of the differences between PENDING and WAITING:

    WAITING is used if there is some time to wait when one service is done and another is to be started.
    PENDING is used if there is some time to wait in one service.

    An transfer consits of multiple sub processesses
    Example:
        Get-Credentials-Subprocess:
            Frontend requests credentials. The gateway creates a new UploadTask with state CREATED.
            After generating the credentials the state is set to WAITING and the credentials are returned.
        MinIO-Upload-Subprocess
            The Frontend uploads the data to MinIO and MinIO notifies the gateway about the successfull transfer.
        File-Import-Subprocess
            The gateway check if there is a UploadTask with state WAITING.
                if yes, the transfer gets continued
                if no, the transfer fails with state ERROR, because there is no according UploadTask for the MinIO Upload
            The gateway has now requests the data from MinIO with state PENDING.
            The Records are created with state IN_PROGRESS
            The Upload is DONE

    So we have two major time slots where the Upload Flow is waiting:
        Between Subprocesses
        In one Subprocess

    I wanted to express that these waiting times are different from each other.
    """

    CREATED = "CREATED"
    WAITING = "WAITING"
    PENDING = "PENDING"
    PREPARED = "PREPARED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    ERROR = "ERROR"


class UploadTypes(Enum):
    DEFAULT = "DEFAULT"
    WORKFLOW_STORE = "WORKFLOW_STORE"
    COGNITION = "COGNITION"


class TokenizerTask(Enum):
    TYPE_DOC_BIN = "DOC_BIN"
    TYPE_TEXT = "TEXT"
    TYPE_TOKEN_STATISTICS = "TOKEN_STATISTICS"
    STATE_CREATED = "CREATED"
    STATE_IN_PROGRESS = "IN_PROGRESS"
    STATE_FINISHED = "FINISHED"
    STATE_FAILED = "FAILED"


class NotificationType(Enum):
    # TASK STATES
    IMPORT_STARTED = "IMPORT_STARTED"
    IMPORT_DONE = "IMPORT_DONE"
    IMPORT_FAILED = "IMPORT_FAILED"
    TOKEN_CREATION_STARTED = "TOKEN_CREATION_STARTED"
    TOKEN_CREATION_DONE = "TOKEN_CREATION_DONE"
    TOKEN_CREATION_FAILED = "TOKEN_CREATION_FAILED"
    EMBEDDING_CREATION_STARTED = "EMBEDDING_CREATION_STARTED"
    EMBEDDING_CREATION_DONE = "EMBEDDING_CREATION_DONE"
    EMBEDDING_CREATION_FAILED = "EMBEDDING_CREATION_FAILED"
    WEAK_SUPERVISION_TASK_STARTED = "WEAK_SUPERVISION_TASK_STARTED"
    WEAK_SUPERVISION_TASK_DONE = "WEAK_SUPERVISION_TASK_DONE"
    WEAK_SUPERVISION_TASK_FAILED = "WEAK_SUPERVISION_TASK_FAILED"
    WEAK_SUPERVISION_TASK_NO_VALID_LABELS = "WEAK_SUPERVISION_TASK_NO_VALID_LABELS"

    INFORMATION_SOURCE_STARTED = "INFORMATION_SOURCE_STARTED"
    INFORMATION_SOURCE_PREPARATION_STARTED = "INFORMATION_SOURCE_PREPARATION_STARTED"
    INFORMATION_SOURCE_COMPLETED = "INFORMATION_SOURCE_COMPLETED"
    PROJECT_DELETED = "PROJECT_DELETED"

    # INFOS
    IMPORT_SAMPLE_PROJECT = "IMPORT_SAMPLE_PROJECT"
    CONVERTING_DATA = "CONVERTING_DATA"
    COLLECTING_SESSION_DATA = "COLLECTING_SESSION_DATA"
    SESSION_INFO = "SESSION_INFO"
    UNKNOWN_DATATYPE = "UNKNOWN_DATATYPE"

    # WARNINGS
    SESSION_RECORD_AMOUNT_CHANGED = "SESSION_RECORD_AMOUNT_CHANGED"
    WRONG_USER_FOR_SESSION = "WRONG_USER_FOR_SESSION"
    DATA_SLICE_ALREADY_EXISTS = "DATA_SLICE_ALREADY_EXISTS"
    MISSING_REFERENCE_DATA = "MISSING_REFERENCE_DATA"
    EMBEDDING_CREATION_WARNING = "EMBEDDING_CREATION_WARNING"
    IMPORT_ISSUES_WARNING = "IMPORT_ISSUES_WARNING"

    # ERRORS
    INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
    INVALID_PRIMARY_KEY = "INVALID_PRIMARY_KEY"
    KNOWLEDGE_BASE_ALREADY_EXISTS = "KNOWLEDGE_BASE_ALREADY_EXISTS"
    TERM_ALREADY_EXISTS = "TERM_ALREADY_EXISTS"
    FILE_TYPE_NOT_GIVEN = "FILE_TYPE_NOT_GIVEN"
    IMPORT_CONVERSION_ERROR = "IMPORT_CONVERSION_ERROR"
    UNKNOWN_PARAMETER = "UNKNOWN_PARAMETER"
    DUPLICATED_COLUMNS = "DUPLICATED_COLUMNS"
    DUPLICATED_TASK_NAMES = "DUPLICATED_TASK_NAMES"
    DUPLICATED_COMPOSITE_KEY = "DUPLICATED_COMPOSITE_KEY"
    DIFFERENTIAL_ATTRIBUTES = "DIFFERENTIAL_ATTRIBUTES"
    NON_EXISTENT_TARGET_ATTRIBUTE = "NON_EXISTENT_TARGET_ATTRIBUTE"
    UPLOAD_CONVERSION_FAILED = "UPLOAD_CONVERSION_FAILED"
    INFORMATION_SOURCE_FAILED = "INFORMATION_SOURCE_FAILED"
    INFORMATION_SOURCE_CANT_FIND_EMBEDDING = "INFORMATION_SOURCE_CANT_FIND_EMBEDDING"
    INFORMATION_SOURCE_S3_EMBEDDING_MISSING = "INFORMATION_SOURCE_S3_EMBEDDING_MISSING"
    INFORMATION_SOURCE_S3_DOCBIN_MISSING = "INFORMATION_SOURCE_S3_DOCBIN_MISSING"
    NEW_ROWS_EXCEED_MAXIMUM_LIMIT = "NEW_ROWS_EXCEED_MAXIMUM_LIMIT"
    TOTAL_ROWS_EXCEED_MAXIMUM_LIMIT = "TOTAL_ROWS_EXCEED_MAXIMUM_LIMIT"
    COLS_EXCEED_MAXIMUM_LIMIT = "COLS_EXCEED_MAXIMUM_LIMIT"
    COL_EXCEED_MAXIMUM_LIMIT = "COL_EXCEED_MAXIMUM_LIMIT"
    DATA_SLICE_CREATION_FAILED = "DATA_SLICE_CREATION_FAILED"
    DATA_SLICE_UPDATE_FAILED = "DATA_SLICE_UPDATE_FAILED"
    BAD_PASSWORD_DURING_IMPORT = "BAD_PASSWORD_DURING_IMPORT"
    RECREATION_OF_EMBEDDINGS_ERROR = "RECREATION_OF_EMBEDDINGS_ERROR"
    DATA_BLOCK_ALREADY_EXISTS = "DATA_BLOCK_EXISTS"
    DATA_BLOCK_NOT_SUPPORTED = "DATA_BLOCK_NOT_SUPPORTED"
    DATA_BLOCK_NOT_FOUND = "DATA_BLOCK_NOT_FOUND"

    # CUSTOM
    CUSTOM = "CUSTOM"


class NotificationState(Enum):
    INITIAL = "INITIAL"
    NOT_INITIAL = "NOT_INITIAL"


class Pages(Enum):
    OVERVIEW = "overview"
    DATA = "data"
    LABELING = "labeling"
    INFORMATION_SOURCES = "heuristics"
    KNOWLEDGE_BASE = "lookup-lists"
    SETTINGS = "settings"
    DATA_BLOCK = "data-blocks"


class SliceTypes(Enum):
    STATIC_DEFAULT = "STATIC_DEFAULT"
    STATIC_OUTLIER = "STATIC_OUTLIER"
    DYNAMIC_DEFAULT = "DYNAMIC_DEFAULT"


class EmbeddingState(Enum):
    INITIALIZING = "INITIALIZING"
    WAITING = "WAITING"
    ENCODING = "ENCODING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class AttributeState(Enum):
    UPLOADED = "UPLOADED"
    AUTOMATICALLY_CREATED = "AUTOMATICALLY_CREATED"
    INITIAL = "INITIAL"
    RUNNING = "RUNNING"
    USABLE = "USABLE"
    FAILED = "FAILED"


class AttributeVisibility(Enum):
    HIDE = "HIDE"  # hide attributes on all pages
    HIDE_ON_LABELING_PAGE = "HIDE_ON_LABELING_PAGE"  # hide attributes on labeling page and data browser page
    HIDE_ON_DATA_BROWSER = "HIDE_ON_DATA_BROWSER"  # hide attributes on data browser page but not on labeling page
    DO_NOT_HIDE = "DO_NOT_HIDE"  # do not hide attributes on any page


class RecordExportFormats(Enum):
    DEFAULT = "DEFAULT"


class RecordImportFileTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    XLSX = "XLSX"


class RecordExportFileTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    XLSX = "XLSX"


class RecordExportAmountTypes(Enum):
    ALL = "ALL"
    SESSION = "SESSION"
    SLICE = "SLICE"


class RecordImportMappingValues(Enum):
    ATTRIBUTE_SPECIFIC = "ATTRIBUTE_SPECIFIC"
    IGNORE = "IGNORE"
    UNKNOWN = "UNKNOWN"


class TokenExpireAtValues(Enum):
    ONE_MONTH = "ONE_MONTH"
    THREE_MONTHS = "THREE_MONTHS"
    NEVER = "NEVER"


class TokenAction(Enum):
    FILE_UPLOAD = "FILE_UPLOAD"


class TokenLimit(Enum):
    FILE_UPLOAD_LIMIT = "FILE_UPLOAD_LIMIT"
    FILE_UPLOAD_INTERVAL = "FILE_UPLOAD_INTERVAL"

    def lowercase(self):
        return self.value.lower()


class TokenScope(Enum):
    READ = "READ"
    READ_WRITE = "READ_WRITE"

    @classmethod
    def all(cls):
        return [e.value for e in cls]


class TokenSubject(Enum):
    PROJECT = Tablenames.PROJECT.value.upper()
    MARKDOWN_DATASET = Tablenames.MARKDOWN_DATASET.value.upper()

    @classmethod
    def all(cls):
        return [e.value for e in cls]


class TokenizationTaskTypes(Enum):
    ATTRIBUTE = "ATTRIBUTE"
    PROJECT = "PROJECT"


class RecordTokenizationScope(Enum):
    PROJECT = "PROJECT"
    ATTRIBUTE = "ATTRIBUTE"


class AdminMessageLevel(Enum):
    WARNING = "WARNING"
    INFO = "INFO"


class TaskType(Enum):
    TOKENIZATION = "tokenization"
    ATTRIBUTE_CALCULATION = "attribute_calculation"
    EMBEDDING = "embedding"
    INFORMATION_SOURCE = "information_source"
    UPLOAD_TASK = "upload"
    WEAK_SUPERVISION = "weak_supervision"
    PARSE_MARKDOWN_FILE = "PARSE_MARKDOWN_FILE"
    PARSE_COGNITION_TMP_FILE = "PARSE_COGNITION_TMP_FILE"
    TASK_QUEUE = "task_queue"
    TASK_QUEUE_ACTION = "task_queue_action"
    RUN_COGNITION_MACRO = "RUN_COGNITION_MACRO"
    PARSE_COGNITION_FILE = "PARSE_COGNITION_FILE"
    EXECUTE_INTEGRATION = "EXECUTE_INTEGRATION"
    EXECUTE_ETL = "EXECUTE_ETL"


class TaskQueueAction(Enum):
    CREATE_OUTLIER_SLICE = "CREATE_OUTLIER_SLICE"
    SEND_WEBSOCKET = "SEND_WEBSOCKET"
    FINISH_COGNITION_SETUP = "FINISH_COGNITION_SETUP"
    RUN_WEAK_SUPERVISION = "RUN_WEAK_SUPERVISION"
    POSTPROCESS_INTEGRATION = "POSTPROCESS_INTEGRATION"


class AgreementType(Enum):
    EMBEDDING = "EMBEDDING"


class EmbeddingPlatform(Enum):
    HUGGINGFACE = "huggingface"
    OPENAI = "openai"
    AZURE = "azure"
    PRIVATEMODE_AI = "privatemode-ai"


class SampleProjectType(Enum):
    CLICKBAIT_INITIAL = "Clickbait - initial"
    CLICKBAIT = "Clickbait"
    AG_NEWS_INITIAL = "AG News - initial"
    AG_NEWS = "AG News"
    CONVERSATIONAL_AI_INITIAL = "Conversational AI - initial"
    CONVERSATIONAL_AI = "Conversational AI"
    DEV_GLOBAL_GUARD_REFERENCES = "Global Guard [References]"
    DEV_GLOBAL_GUARD_QUESTIONS = "Global Guard [Questions]"


class StrategyStepType(Enum):
    NONE = "NONE"
    PYTHON = "PYTHON"
    LLM = "LLM"
    SELECTION = "SELECTION"
    # now more like a common llm node but changing the enum value would break the existing data
    QUERY_REPHRASING = "QUERY_REPHRASING"
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # WEBSEARCH = "WEBSEARCH" # done in exec env to ensure security
    TRUNCATE_CONTEXT = "TRUNCATE_CONTEXT"
    HEADER = "HEADER"
    # INFO: done in exec env to prevent installing sklearn in gateway
    TMP_DOC_RETRIEVAL = "TMP_DOC_RETRIEVAL"
    CALL_OTHER_AGENT = "CALL_OTHER_AGENT"
    # INFO: will replace retrieval in the future, direct access to neural search without gates
    NEURAL_SEARCH = "NEURAL_SEARCH"
    WEBHOOK = "WEBHOOK"
    GRAPHRAG_SEARCH = "GRAPHRAG_SEARCH"
    TEMPLATED = "TEMPLATED"
    RERANKER = "RERANKER"
    FULL_TEXT_SEARCH = "FULL_TEXT_SEARCH"
    CURRENT_TIME = "CURRENT_TIME"
    COMPLIANT_WEBSEARCH = "COMPLIANT_WEBSEARCH"
    DATA_BLOCK = "DATA_BLOCK"

    def get_description(self):
        return STEP_DESCRIPTIONS.get(self, "No description available")

    def get_when_to_use(self):
        return STEP_WHEN_TO_USE.get(self, "No description available")

    def get_progress_text(self):
        return STEP_PROGRESS_TEXTS.get(self, "No progress text available")


STEP_DESCRIPTIONS = {
    StrategyStepType.NEURAL_SEARCH: "Fetch facts from an embedding",
    # StrategyStepType.RELEVANCE: "Classify retrieved facts",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.PYTHON: "Custom python function",
    StrategyStepType.LLM: "Answer with LLM",
    StrategyStepType.SELECTION: "Select data",
    StrategyStepType.QUERY_REPHRASING: "Transform with LLM",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "Search the web",
    StrategyStepType.TRUNCATE_CONTEXT: "Truncate context",
    StrategyStepType.HEADER: "Writing header",
    StrategyStepType.TMP_DOC_RETRIEVAL: "Temporary document retrieval",
    StrategyStepType.CALL_OTHER_AGENT: "Retrieve results from other agents",
    StrategyStepType.WEBHOOK: "Webhook",
    StrategyStepType.GRAPHRAG_SEARCH: "Query GraphRAG index",
    StrategyStepType.TEMPLATED: "Templated step",
    StrategyStepType.RERANKER: "Reranker",
    StrategyStepType.FULL_TEXT_SEARCH: "Full text search",
    StrategyStepType.CURRENT_TIME: "Get current time",
    StrategyStepType.COMPLIANT_WEBSEARCH: "Web search",
    StrategyStepType.DATA_BLOCK: "Create a data block",
}

STEP_WHEN_TO_USE = {
    StrategyStepType.NEURAL_SEARCH: "When you want to fetch facts based on an embedding",
    StrategyStepType.PYTHON: "When you want to run a custom python function",
    StrategyStepType.LLM: "When you want to give an actual answer to the question",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.SELECTION: "When you want to select data",
    StrategyStepType.QUERY_REPHRASING: "When you want to manipulate or extend your attributes with a LLM",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "When you want to search the web",
    StrategyStepType.TRUNCATE_CONTEXT: "When you want to truncate context",
    StrategyStepType.HEADER: "When you want to set a header based on the conversation",
    StrategyStepType.TMP_DOC_RETRIEVAL: "When you want to retrieve results from conversation specific documents",
    StrategyStepType.CALL_OTHER_AGENT: "When you want to call another agent",
    StrategyStepType.WEBHOOK: "When you want to run a webhook",
    StrategyStepType.GRAPHRAG_SEARCH: "When you want to query a knowledge graph",
    StrategyStepType.TEMPLATED: "When you want to reuse existing templates",
    StrategyStepType.RERANKER: "When you want to rerank results",
    StrategyStepType.FULL_TEXT_SEARCH: "When you want to perform a full text search",
    StrategyStepType.CURRENT_TIME: "When you want to get the current time",
    StrategyStepType.COMPLIANT_WEBSEARCH: "When you want to perform a web search",
    StrategyStepType.DATA_BLOCK: "When you want to understand how the project is used",
}

STEP_PROGRESS_TEXTS = {
    StrategyStepType.NEURAL_SEARCH: "Retrieving facts",
    # StrategyStepType.RELEVANCE: "Classifying facts",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.PYTHON: "Running custom python function",
    StrategyStepType.LLM: "Running LLM",
    StrategyStepType.SELECTION: "Selecting data",
    StrategyStepType.QUERY_REPHRASING: "Rephrasing query",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "Searching the web",
    StrategyStepType.TRUNCATE_CONTEXT: "Truncating context",
    StrategyStepType.HEADER: "Headline generation",
    StrategyStepType.TMP_DOC_RETRIEVAL: "Retrieving facts from conversation specific documents",
    StrategyStepType.CALL_OTHER_AGENT: "Calling another agent",
    StrategyStepType.WEBHOOK: "Running webhook",
    StrategyStepType.GRAPHRAG_SEARCH: "Querying knowledge graph",
    StrategyStepType.TEMPLATED: "Running templated step",
    StrategyStepType.RERANKER: "Running reranker",
    StrategyStepType.FULL_TEXT_SEARCH: "Running full text search",
    StrategyStepType.CURRENT_TIME: "Getting current time",
    StrategyStepType.COMPLIANT_WEBSEARCH: "Searching the web",
    StrategyStepType.DATA_BLOCK: "Evaluating data block",
}

STEP_ERRORS = {
    StrategyStepType.HEADER.value: "Header must come after field answer is set in the record_dict.",
}


class PipelineStep(Enum):
    INCOMING_QUESTION = "INCOMING_QUESTION"
    INCOMING_QUESTION_TRY = "INCOMING_QUESTION_TRY"
    ROUTE_STRATEGY = "ROUTE_STRATEGY"
    STRATEGY_STEP = "STRATEGY_STEP"
    MAPPING_BEFORE_STRATEGY = "MAPPING_BEFORE_STRATEGY"
    MAPPING_AFTER_STRATEGY = "MAPPING_AFTER_STRATEGY"
    ASSISTANT_RESPONSE = "ASSISTANT_RESPONSE"


class PipelineStepState(Enum):
    STARTED = "STARTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class MarkdownFileCategoryOrigin(Enum):
    PDF = "PDF"
    WEB = "WEB"
    SPREADSHEET = "SPREADSHEET"
    DOCUMENTS = "DOCUMENTS"


class RefinerySynchronizationTaskState(Enum):
    CREATED = "CREATED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class RefinerySynchronizationIntervalOption(Enum):
    NEVER = "NEVER"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class LLMProvider(Enum):
    OPENAI = "Open AI"
    AZURE = "Azure"
    AZURE_FOUNDRY = "Azure Foundry"
    PRIVATEMODE_AI = "Privatemode AI"

    @staticmethod
    def from_string(value: str):
        changed_value = value.upper().replace(" ", "_").replace("-", "_")
        if changed_value == "OPEN_AI":
            return LLMProvider.OPENAI
        elif changed_value == "AZURE":
            return LLMProvider.AZURE
        elif changed_value == "AZURE_FOUNDRY":
            return LLMProvider.AZURE_FOUNDRY
        elif changed_value == "PRIVATEMODE_AI":
            return LLMProvider.PRIVATEMODE_AI
        raise ValueError("Could not parse LLMProvider from string")

    def as_key(self):
        return self.value.replace(" ", "_").upper()


# now also etl states!
class CognitionMarkdownFileState(EnumKern):
    QUEUE = "QUEUE"
    STARTED = "STARTED"
    EXTRACTING = "EXTRACTING"
    TOKENIZING = "TOKENIZING"
    SPLITTING = "SPLITTING"
    TRANSFORMING = "TRANSFORMING"
    LOADING = "LOADING"  # e.g. to file reference in db
    # CACHE_HANDLING = "CACHE_HANDLING"
    NOTIFYING = (
        "NOTIFYING"  # e.g. notifying that the file is ready of integration provider
    )
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class CognitionInterfaceType(Enum):
    CHAT = "CHAT"
    COMPARE = "COMPARE"
    ENRICH = "ENRICH"


class EmitType(Enum):
    ANSWER = "ANSWER"
    RETRIEVAL_RESULTS = "RETRIEVAL_RESULTS"
    FOLLOW_UPS = "FOLLOW_UPS"
    SELECTION = "SELECTION"
    QUERY_REPHRASING = "QUERY_REPHRASING"


class CognitionProjectState(Enum):
    CREATED = "CREATED"
    WIZARD_RUNNING = "WIZARD_RUNNING"
    DEVELOPMENT = "DEVELOPMENT"
    PRODUCTION = "PRODUCTION"


class StrategyComplexity(Enum):
    SIMPLE = "SIMPLE"
    REGULAR = "REGULAR"
    COMPLEX = "COMPLEX"


class ConsumptionLogState(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class CognitionConfigKey(Enum):
    STRATEGY_COMPLEXITY_THRESHOLD = "STRATEGY_COMPLEXITY_THRESHOLD"
    STRATEGY_STEP_WEIGHTS = "STRATEGY_STEP_WEIGHTS"


# note this is only for websocket interaction between exec env and gateway
# none of these can/is allowed to interact with the database or anything other than the websocket!
# means if you want a live update and set it as answer this needs to be done in the exec env code record_dict change
class AllowedExecEnvMessageTypes(Enum):
    CHUNK = "CHUNK"  # sends a chunk to the ui - same as llm step type
    SET_UI_MESSAGE = "SET_UI_MESSAGE"  # replaces answer in the ui
    CLOSE = "CLOSE"  # closes the websocket - shouldn't be sent by hand!


def try_parse_enum_value(string: str, enumType: Enum, raise_me: bool = True) -> Any:
    try:
        parsed = enumType[string.upper()]
    except KeyError:
        if raise_me:
            raise ValueError(f"Invalid value {string} for enum {enumType}")
        return
    return parsed


# this is only a method check.
# Some endpoints were implemented as POST requests to allow a body even though they are not creating anything
# Example comment/all-comments. For these i added a dependency extend_state_get_like with sets a state variable to use as indicator
class AdminLogLevel(Enum):
    DONT_LOG = "DONT_LOG"  # nothing is logged
    NO_GET = "NO_GET"  # everything but method GET is logged
    ALL = "ALL"  # everything is logged

    def log_me(self, method: str) -> bool:
        if self == AdminLogLevel.DONT_LOG:
            return False
        if self == AdminLogLevel.NO_GET and method == "GET":
            return False
        return True


# currently only one option, but could be extended in the future
class MacroType(Enum):
    # macro is meant to be run on a (or n) documents
    DOCUMENT_MESSAGE_QUEUE = "DOCUMENT_MESSAGE_QUEUE"
    FOLDER_MESSAGE_QUEUE = "FOLDER_MESSAGE_QUEUE"

    @classmethod
    def all(cls):
        return [e.value for e in cls]


# currently only one option, but could be extended in the future
class MacroNodeContentType(Enum):
    # add a new question to the conversation
    CONVERSATION_QUESTION = "CONVERSATION_QUESTION"


# currently only one option, but could be extended in the future
class MacroEdgeConditionType(Enum):
    # all edges from a given node are collected and an llm decides which to chose based on the name of the edge
    LLM_SELECTION = "LLM_SELECTION"


class MacroScope(Enum):
    ADMIN = "ADMIN"
    ORGANIZATION = "ORGANIZATION"
    PROJECT = "PROJECT"


class MacroState(Enum):
    DEVELOPMENT = "DEVELOPMENT"
    PRODUCTION = "PRODUCTION"


class MacroExecutionState(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class MacroExecutionLinkAction(Enum):
    CREATE = "CREATE"
    DELETE = "DELETE"
    UPDATE = "UPDATE"


class AdminMacrosDisplay(Enum):
    DONT_SHOW = "DONT_SHOW"
    FOR_ADMINS = "FOR_ADMINS"
    FOR_ENGINEERS = "FOR_ENGINEERS"
    FOR_ALL = "FOR_ALL"


class CustomerButtonType(Enum):

    DATA_MAPPER = "DATA_MAPPER"
    # sends data to the data mapper, needs to ensure the request has the key included!

    # ______________________________
    # extended on demand over time


class CustomerButtonLocation(Enum):
    COGNITION_MACRO_RESULTS_TABLE = "COGNITION_MACRO_RESULTS_TABLE"  # url /macros/<macro_id> # only visible if meta data display is active

    # extended on demand over time


class FileCachingInitiator(Enum):
    TMP_DOC_RETRIEVAL = "TMP_DOC_RETRIEVAL"
    DATASET_MARKDOWN_FILE = "DATASET_MARKDOWN_FILE"


class FileCachingState(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


# UPLOAD_EXTRACT_TRANSFORM --> File was never uploaded, will be newly uploaded, extracted and transformed (Nothing exists)
# EXTRACT_TRANSFORM --> File was uploaded before, will be newly extracted and transformed (FileReference exists)
# TRANSFORM --> File was uploaded and extracted before with given config and will be newly transformed (FileReference + FileExtraction exists)
# CACHE --> File was uploaded, extracted and transformed before with given config and nothing will be recalculated(FileReference + FileExtraction + FileTransformation exists)
class FileCachingProcessingScope(Enum):
    UPLOAD_EXTRACT_TRANSFORM = "UPLOAD_EXTRACT_TRANSFORM"
    EXTRACT_TRANSFORM = "EXTRACT_TRANSFORM"
    TRANSFORM = "TRANSFORM"
    CACHE = "CACHE"


class ChangeAction(Enum):
    KEY_REMOVED = "KEY_REMOVED"
    KEY_ADDED = "KEY_ADDED"
    VALUE_CHANGED = "VALUE_CHANGED"
    LIST_VALUE_CHANGED = "LIST_VALUE_CHANGED"
    ITEM_REMOVED = "ITEM_REMOVED"
    ITEM_ADDED = "ITEM_ADDED"


class PipelineVersionType(Enum):
    AUTO_SAVE = "AUTO_SAVE"  # any save operation in relevant but only 10 per project
    NAMED_VERSION = "NAMED_VERSION"  # any AUTO_SAVE that is considered worth keeping


class GraphRAGIndexState(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EvaluationRunState(Enum):
    INITIATED = "INITIATED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class AdminQueries(Enum):
    # default values for parameters can be found in file admin_queries.py
    USERS_TO_PROJECTS = (
        "USERS_TO_PROJECTS"  # parameter options: organization_id, without_kern_email
    )
    USERS_BY_ORG = (
        "USERS_BY_ORG"  # parameter options: organization_id, without_kern_email
    )
    ACTIVE_USERS_GLOBAL = "ACTIVE_USERS_GLOBAL"  # parameter options: min_msg_count, period (days, weeks or months), slices, organization_id, without_kern_email
    ACTIVE_USERS_BY_ORG = "ACTIVE_USERS_BY_ORG"  # parameter options: min_msg_count, period (days, weeks or months), slices, organization_id, without_kern_email
    MESSAGES_CREATED = "MESSAGES_CREATED"  # parameter options: period (days, weeks or months), slices, organization_id, without_kern_email
    MESSAGES_CREATED_BY_PROJECT = "MESSAGES_CREATED_BY_PROJECT"  # parameter options: period (days, weeks or months), slices, organization_id, without_kern_email
    MESSAGES_FEEDBACK_PER_PROJECT = "MESSAGES_FEEDBACK_PER_PROJECT"  # parameter options: period (days, weeks or months), slices, organization_id, without_kern_email
    AVG_MESSAGES_PER_CONVERSATION_GLOBAL = "AVG_MESSAGES_PER_CONVERSATION_GLOBAL"  # parameter options: organization_id, without_kern_email
    AVG_MESSAGES_PER_CONVERSATION = "AVG_MESSAGES_PER_CONVERSATION"  # parameter options: period (days, weeks or months), slices, organization_id, without_kern_email
    MACRO_EXECUTIONS = "MACRO_EXECUTIONS"  # parameter options: period (days, weeks or months), slices, organization_id, without_kern_email
    FOLDER_MACRO_EXECUTION_SUMMARY = (
        "FOLDER_MACRO_EXECUTION_SUMMARY"  # parameter options: organization_id
    )
    CREATED_TAGS_PER_ORG = (
        "CREATED_TAGS_PER_ORG"  # parameter options: organization_id, without_kern_email
    )
    CONVERSATIONS_PER_TAG = "CONVERSATIONS_PER_TAG"  # parameter options: organization_id, without_kern_email, distinct_conversations
    MULTITAGGED_CONVERSATIONS = "MULTITAGGED_CONVERSATIONS"  # parameter options: organization_id, without_kern_email
    TEMPLATE_USAGE = "TEMPLATE_USAGE"  # parameter options:  organization_id
    PRIVATEMODE_USE_OVER_TIME = "PRIVATEMODE_USE_OVER_TIME"  # parameter options: organization_id, without_kern_email
    INCOGNITO_USE_OVER_TIME = "INCOGNITO_USE_OVER_TIME"  # parameter options: organization_id, without_kern_email


class CognitionIntegrationType(Enum):
    SHAREPOINT = "SHAREPOINT"
    GITHUB_FILE = "GITHUB_FILE"
    GITHUB_ISSUE = "GITHUB_ISSUE"
    PDF = "PDF"

    @staticmethod
    def from_string(value: str):
        changed_value = value.upper().replace(" ", "_").replace("-", "_")
        try:
            return CognitionIntegrationType[changed_value]
        except KeyError:
            raise KeyError(
                f"Could not parse CognitionIntegrationType from string '{changed_value}'"
            )


class SharepointPropertySyncState(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CognitionPrivateUsage(Enum):
    KERN_INTERNAL = "KERN_INTERNAL"  # same server no connection outside VM & DB
    KERN_EXTERNAL_RESOURCE = (
        "KERN_EXTERNAL_RESOURCE"  # e.g. reranker, distinct from external
    )
    WEBHOOK_NO_DATA = "WEBHOOK_NO_DATA"  # webhook only sending conversation id
    DATA_SEND_EXTERNALLY = (
        "DATA_SEND_EXTERNALLY"  # e.g. webhook with data or reranker outside kern
    )
    AZURE = "AZURE"  # e.g. BYOK from customers
    AZURE_KERN = "AZURE_KERN"  # e.g. sponsorship azure resources or e.g. mistral with key specific key
    OPEN_AI = "OPEN_AI"  # hosted by openai
    PRIVATEMODE_AI = "PRIVATEMODE_AI"  # encrypted and hosted by privatemode.ai
    REQUESTS_USED = "REQUESTS_USED"  # special case, is set handled separately => only for final result set
    COMPLIANT_WEBSEARCH_PROVIDER = "COMPLIANT_WEBSEARCH_PROVIDER"  # e.g. staan.ai
    __SCORES = {
        "KERN_INTERNAL": 1.0,
        "KERN_EXTERNAL_RESOURCE": 0.95,
        "WEBHOOK_NO_DATA": 0.9,
        "DATA_SEND_EXTERNALLY": 0.3,
        "AZURE": 0.5,
        "AZURE_KERN": 0.85,
        "OPEN_AI": 0.3,
        "PRIVATEMODE_AI": 0.95,
        "COMPLIANT_WEBSEARCH_PROVIDER": 0.85,
        "REQUESTS_USED": 0.9,  # final project multiplier
    }

    @property
    def score(self) -> float:
        return self.__SCORES[self.value]


class MessageInitiationType(Enum):
    UI = "UI"
    API = "API"
    MACRO = "MACRO"


class MessageType(Enum):
    WITH_ERROR = "WITH_ERROR"
    WITHOUT_ERROR = "WITHOUT_ERROR"
    ALL = "ALL"


class TimedExecutionKey(Enum):
    LAST_RESET_USER_MESSAGE_COUNT = "LAST_RESET_USER_MESSAGE_COUNT"


class ETLSplitStrategy(EnumKern):
    CHUNK = "CHUNK"
    SHRINK = "SHRINK"


class ETLFileType(Enum):
    DEFAULT = "TXT"
    MD = "MD"
    TXT = "TXT"
    PDF = "PDF"
    WORD = "WORD"
    EXCEL = "EXCEL"
    POWERPOINT = "POWERPOINT"
    IMG = "IMG"

    @classmethod
    def from_string(cls, value: str):
        changed_value = value.upper().replace(" ", "_").replace("-", "_")
        for member in cls:
            if member.value == changed_value:
                return member
        return cls.TXT

    def get_supported_file_extensions(self) -> List[str]:
        if self == ETLFileType.MD:
            return [".md", ".markdown", ".mdown", ".mkdn", ".mkd"]
        elif self == ETLFileType.PDF:
            return [".pdf"]
        elif self == ETLFileType.WORD:
            return [".docx", ".doc"]
        elif self == ETLFileType.EXCEL:
            return [".xlsx", ".xls"]
        elif self == ETLFileType.POWERPOINT:
            return [".pptx", ".ppt"]
        elif self == ETLFileType.IMG:
            return [
                ".png",
                ".jpg",
                ".jpeg",
                ".gif",
                ".bmp",
                ".tiff",
                ".webp",
                ".avif",
            ]
        return [".txt"]

    @staticmethod
    def get_all_supported_file_extensions():
        all_supported_file_extensions = []
        for FileType in ETLFileType:
            all_supported_file_extensions.extend(
                FileType.get_supported_file_extensions()
            )
        return all_supported_file_extensions

    @staticmethod
    def from_extension(value: str):
        changed_value = value.lower()
        if changed_value in [".md", ".markdown", ".mdown", ".mkdn", ".mkd"]:
            return ETLFileType.MD
        elif changed_value in [".pdf"]:
            return ETLFileType.PDF
        elif changed_value in [".docx", ".doc"]:
            return ETLFileType.WORD
        elif changed_value in [".xlsx", ".xls"]:
            return ETLFileType.EXCEL
        elif changed_value in [".pptx", ".ppt"]:
            return ETLFileType.POWERPOINT
        elif changed_value in [
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".bmp",
            ".tiff",
            ".webp",
            ".avif",
        ]:
            return ETLFileType.IMG
        # default is treated like txt so no extension mapping needed
        else:
            return ETLFileType.DEFAULT

    @staticmethod
    def from_mimetype(value: str):
        changed_value = value.lower()

        if changed_value in ["application/pdf"]:
            return ETLFileType.PDF
        elif changed_value in [
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",
        ]:
            return ETLFileType.WORD
        elif changed_value in [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
        ]:
            return ETLFileType.EXCEL
        elif changed_value in [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.ms-powerpoint",
        ]:
            return ETLFileType.POWERPOINT
        elif changed_value.startswith("image/"):
            return ETLFileType.IMG
        else:
            return ETLFileType.DEFAULT

    @classmethod
    def get_default_extractor(cls, file_type: Optional["ETLFileType"] = None):
        if file_type == ETLFileType.MD:
            return ETLExtractorMD.FILESYSTEM
        elif file_type == ETLFileType.PDF:
            # integrations can exhaust cognition-pdf2md
            # return ETLExtractorPDF.PDF2MD
            return ETLExtractorPDF.LANGCHAIN
        elif file_type == ETLFileType.WORD:
            return ETLExtractorWord.LANGCHAIN
        elif file_type == ETLFileType.EXCEL:
            return ETLExtractorExcel.LANGCHAIN
        elif file_type == ETLFileType.POWERPOINT:
            return ETLExtractorPowerpoint.LANGCHAIN
        elif file_type == ETLFileType.IMG:
            return ETLExtractorImg.LANGCHAIN
        elif file_type == ETLFileType.DEFAULT or file_type == ETLFileType.TXT:
            return ETLExtractorTxt.LANGCHAIN
        raise ValueError(f"No default extractor for given file type {file_type}")

    def get_extractor_from_string(self, extractor: Optional[str] = None) -> EnumKern:
        if extractor is None:
            return self.get_default_extractor(self)
        if self == ETLFileType.MD:
            return ETLExtractorMD.from_string(extractor)
        elif self == ETLFileType.PDF:
            return ETLExtractorPDF.from_string(extractor)
        elif self == ETLFileType.WORD:
            return ETLExtractorWord.from_string(extractor)
        elif self == ETLFileType.EXCEL:
            return ETLExtractorExcel.from_string(extractor)
        elif self == ETLFileType.POWERPOINT:
            return ETLExtractorPowerpoint.from_string(extractor)
        elif self == ETLFileType.IMG:
            return ETLExtractorImg.from_string(extractor)
        return self.get_default_extractor(self)

    def get_supported_extractors(self) -> List[str]:
        if self == ETLFileType.MD:
            return ETLExtractorMD.all()
        elif self == ETLFileType.PDF:
            return ETLExtractorPDF.all()
        elif self == ETLFileType.WORD:
            return ETLExtractorWord.all()
        elif self == ETLFileType.EXCEL:
            return ETLExtractorExcel.all()
        elif self == ETLFileType.POWERPOINT:
            return ETLExtractorPowerpoint.all()
        elif self == ETLFileType.IMG:
            return ETLExtractorImg.all()
        return ETLExtractorTxt.all()


class ETLExtractorMD(EnumKern):
    LANGCHAIN = "LANGCHAIN"
    FILESYSTEM = "FILESYSTEM"


class ETLExtractorPDF(EnumKern):
    LANGCHAIN = "LANGCHAIN"
    VISION = "VISION"
    AZURE_DI = "AZURE_DI"
    PDF2MD = "PDF2MD"

    @classmethod
    def from_string(cls, value: Optional[str]):
        if value is None:
            return cls.LANGCHAIN
        changed_value = value.upper().replace(" ", "_").replace("-", "_")
        for member in cls:
            if member.value == changed_value:
                return member
        if changed_value == "PDF2MARKDOWN":
            return cls.PDF2MD
        if changed_value == "GPT_4":
            return cls.VISION
        return cls.VISION


class ETLExtractorWord(EnumKern):
    LANGCHAIN = "LANGCHAIN"


class ETLExtractorExcel(EnumKern):
    LANGCHAIN = "LANGCHAIN"


class ETLExtractorPowerpoint(EnumKern):
    LANGCHAIN = "LANGCHAIN"


class ETLExtractorImg(EnumKern):
    LANGCHAIN = "LANGCHAIN"


class ETLExtractorTxt(EnumKern):
    LANGCHAIN = "LANGCHAIN"


class ETLExtractors:
    def get_all_extractors() -> Dict[EnumKern, List[str]]:
        all_extractors = {}
        for file_type in ETLFileType:
            all_extractors[file_type] = file_type.get_supported_extractors()
        return all_extractors


class ETLTransformer(EnumKern):
    SUMMARIZE = "SUMMARIZE"
    CLEANSE = "CLEANSE"
    TEXT_TO_TABLE = "TEXT_TO_TABLE"


class ETLTransformerType(EnumKern):
    COMMON_ETL = "COMMON_ETL"
    NO_TRANSFORMATION = "NO_TRANSFORMATION"
    SUMMARIZE = "SUMMARIZE"

    # backwards compatibility
    @classmethod
    def from_transformers(
        cls, transformers: List[Dict[str, Any]]
    ) -> "ETLTransformerType":
        if not transformers or len(transformers) == 0:
            return cls.NO_TRANSFORMATION
        if (
            len(transformers) == 1
            and transformers[0]["type"] == ETLTransformer.SUMMARIZE.value
        ):
            return cls.SUMMARIZE
        return cls.COMMON_ETL


class InboxMailThreadSupportProgressState(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"
    FAILED = "FAILED"


class DataBlockType(EnumKern):
    LIVE = "LIVE"
    STABLE = "STABLE"
