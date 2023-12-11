from enum import Enum


class DataTypes(Enum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    CATEGORY = "CATEGORY"
    TEXT = "TEXT"
    EMBEDDING_LIST = "EMBEDDING_LIST"  # only for embeddings & default hidden
    UNKNOWN = "UNKNOWN"


class ProjectStatus(Enum):
    INIT_UPLOAD = "INIT_UPLOAD"
    INIT_COMPLETE = "INIT_COMPLETE"
    IN_DELETION = "IN_DELETION"
    INIT_SAMPLE_PROJECT = "INIT_SAMPLE_PROJECT"


class RecordCategory(Enum):
    SCALE = "SCALE"
    TEST = "TEST"


class LabelSource(Enum):
    MANUAL = "MANUAL"
    # WEAK_SUPERVISION = Output of the Weak Supervision Model - ehemeals "programmatic"
    WEAK_SUPERVISION = "WEAK_SUPERVISION"
    INFORMATION_SOURCE = "INFORMATION_SOURCE"
    MODEL_CALLBACK = "MODEL_CALLBACK"


class InformationSourceType(Enum):
    LABELING_FUNCTION = "LABELING_FUNCTION"
    ACTIVE_LEARNING = "ACTIVE_LEARNING"
    PRE_COMPUTED = "PRE_COMPUTED"
    ZERO_SHOT = "ZERO_SHOT"
    CROWD_LABELER = "CROWD_LABELER"


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

    # for crowd labelers, there is a slightly different state flow
    STARTED = "STARTED"


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
    USER_ACTIVITY = "user_activity"
    UPLOAD_TASK = "upload_task"
    DATA_SLICE = "data_slice"
    DATA_SLICE_RECORD_ASSOCIATION = "data_slice_record_association"
    INFORMATION_SOURCE_STATISTICS_EXCLUSION = "information_source_statistics_exclusion"
    COMMENT_DATA = "comment_data"
    LABELING_ACCESS_LINK = "labeling_access_link"
    PERSONAL_ACCESS_TOKEN = "personal_access_token"
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
    REFINERY_SYNCHRONIZATION_TASK = "refinery_synchronization_task"
    PYTHON_STEP = "python_step"
    LLM_STEP = "llm_step"
    MARKDOWN_LLM_LOGS = "markdown_llm_logs"
    MARKDOWN_DATASET = "markdown_dataset"
    SELECTION_STEP = "selection_step"
    WEBSEARCH_STEP = "websearch_step"

    def snake_case_to_pascal_case(self):
        # the type name of a table is needed to create backrefs
        # in order to call them via GraphQL; type names are written in PascalCase
        return "".join([word.title() for word in self.value.split("_")])

    def snake_case_to_camel_case(self):
        # GraphQL needs camel case for resolving
        return "".join(
            [
                word.title() if idx > 0 else word.lower()
                for idx, word in enumerate(self.value.split("_"))
            ]
        )


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


class ConfusionMatrixElements(Enum):
    OUTSIDE = "Outside"


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
    LABEL_STUDIO = "LABEL_STUDIO"
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


class DOCS(Enum):
    UPLOADING_DATA = "https://docs.kern.ai/refinery/project-creation-and-data-upload"
    KNOWLEDGE_BASE = "https://docs.kern.ai/refinery/heuristics#labeling-functions"
    WORKFLOW = "https://docs.kern.ai/refinery/manual-labeling#labeling-workflow"
    CREATING_PROJECTS = "https://docs.kern.ai/refinery/project-creation-and-data-upload#project-creation-workflow"
    WEAK_SUPERVISION = "https://docs.kern.ai/refinery/weak-supervision"
    CREATE_EMBEDDINGS = "https://docs.kern.ai/refinery/embedding-integration"
    INFORMATION_SOURCES = "https://docs.kern.ai/refinery/heuristics#labeling-functions"
    DATA_BROWSER = "https://docs.kern.ai/refinery/data-management"


class SliceTypes(Enum):
    STATIC_DEFAULT = "STATIC_DEFAULT"
    STATIC_OUTLIER = "STATIC_OUTLIER"
    DYNAMIC_DEFAULT = "DYNAMIC_DEFAULT"


class InterAnnotatorConstants(Enum):
    ID_GOLD_USER = "GOLD_STAR"
    ID_NULL_USER = "NULL_USER"


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
    LABEL_STUDIO = "LABEL_STUDIO"


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


class TokenScope(Enum):
    READ = "READ"
    READ_WRITE = "READ_WRITE"


class TokenizationTaskTypes(Enum):
    ATTRIBUTE = "ATTRIBUTE"
    PROJECT = "PROJECT"


class RecordTokenizationScope(Enum):
    PROJECT = "PROJECT"
    ATTRIBUTE = "ATTRIBUTE"


class GatesIntegrationStatus(Enum):
    READY = "READY"
    NOT_READY = "NOT_READY"
    UPDATING = "UPDATING"


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
    TASK_QUEUE = "task_queue"
    TASK_QUEUE_ACTION = "task_queue_action"


class TaskQueueAction(Enum):
    CREATE_OUTLIER_SLICE = "CREATE_OUTLIER_SLICE"
    START_GATES = "START_GATES"
    SEND_WEBSOCKET = "SEND_WEBSOCKET"
    FINISH_COGNITION_SETUP = "FINISH_COGNITION_SETUP"
    RUN_WEAK_SUPERVISION = "RUN_WEAK_SUPERVISION"


class AgreementType(Enum):
    EMBEDDING = "EMBEDDING"


class EmbeddingPlatform(Enum):
    PYTHON = "python"
    HUGGINGFACE = "huggingface"
    COHERE = "cohere"
    OPENAI = "openai"
    AZURE = "azure"


class SampleProjectType(Enum):
    CLICKBAIT_INITIAL = "Clickbait - initial"
    CLICKBAIT = "Clickbait"
    AG_NEWS_INITIAL = "AG News - initial"
    AG_NEWS = "AG News"
    CONVERSATIONAL_AI_INITIAL = "Conversational AI - initial"
    CONVERSATIONAL_AI = "Conversational AI"


class StrategyStepType(Enum):
    RETRIEVAL = "RETRIEVAL"
    RELEVANCE = "RELEVANCE"
    NONE = "NONE"
    PYTHON = "PYTHON"
    LLM = "LLM"
    SELECTION = "SELECTION"
    QUERY_REPHRASING = "QUERY_REPHRASING"
    WEBSEARCH = "WEBSEARCH"

    def get_description(self):
        return STEP_DESCRIPTIONS.get(self, "No description available")
    
    def get_progress_text(self):
        return STEP_PROGRESS_TEXTS.get(self, "No progress text available")


STEP_DESCRIPTIONS = {
    StrategyStepType.RETRIEVAL: "Fetch facts from a DB",
    StrategyStepType.RELEVANCE: "Classify retrieved facts",
    StrategyStepType.PYTHON: "Custom python function",
    StrategyStepType.LLM: "Run a LLM",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.SELECTION: "Select data",
    StrategyStepType.QUERY_REPHRASING: "Rephrase query",
    StrategyStepType.WEBSEARCH: "Search the web",
}

STEP_PROGRESS_TEXTS = {
    StrategyStepType.RETRIEVAL: "Retrieving facts",
    StrategyStepType.RELEVANCE: "Classifying facts",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.PYTHON: "Running custom python function",
    StrategyStepType.LLM: "Running LLM",
    StrategyStepType.SELECTION: "Selecting data",
    StrategyStepType.QUERY_REPHRASING: "Rephrasing query",
    StrategyStepType.WEBSEARCH: "Searching the web",
}


class PipelineSteps(Enum):
    INCOMING_QUESTION = "INCOMING_QUESTION"
    INCOMING_QUESTION_TRY = "INCOMING_QUESTION_TRY"
    QUESTION_ENRICHMENT = "QUESTION_ENRICHMENT"
    ROUTE_STRATEGY = "ROUTE_STRATEGY"
    STRATEGY_STEP = "STRATEGY_STEP"
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
    OPEN_SOURCE = "Open-Source"
    AZURE = "Azure"

class CognitionMarkdownFileState(Enum):
    CREATED = "CREATED"
    SPLITTING = "SPLITTING"
    TRANSFORMING = "TRANSFORMING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class EmitType(Enum):
    ANSWER = "ANSWER"
    RETRIEVAL_RESULTS = "RETRIEVAL_RESULTS"
    FOLLOW_UPS = "FOLLOW_UPS"
    SELECTION = "SELECTION"
    QUERY_REPHRASING = "QUERY_REPHRASING"

class CognitionLLMStepUsageType(Enum):
    BASE = "BASE"
    QUERY_REPHRASING = "QUERY_REPHRASING"